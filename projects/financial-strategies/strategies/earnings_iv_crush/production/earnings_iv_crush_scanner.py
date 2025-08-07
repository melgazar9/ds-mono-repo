from ib_async import IB, Stock, Contract
import warnings
import pandas as pd
from datetime import datetime, timedelta
from scipy.interpolate import interp1d
import numpy as np
import argparse
import logging


class MarketDataExtractor:
    def __init__(self, ticker, exchange, currency):
        self.ticker = ticker
        self.exchange = exchange
        self.currency = currency

        self.contract = None
        self.qualified_contracts = None
        self.df_1min = None
        self.df_90days = None
        self.option_chain_definitions = None

        self._setup_contract()

    def _setup_contract(self):
        self.contract = Stock(self.ticker, exchange=self.exchange, currency=self.currency)
        self.qualified_contracts = ib.qualifyContracts(self.contract)

        if not self.qualified_contracts:
            logging.error("Could not qualify contract")
            ib.disconnect()
            return None

        if len(self.qualified_contracts) > 1:
            warnings.warn("More than one contract qualified for ticker {}! {}".format(self.ticker, self.qualified_contracts))

        self.contract = self.qualified_contracts[0]
        return self.contract

    def get_1min_bars(self):
        bars_1min = ib.reqHistoricalData(
            self.contract,
            endDateTime="",
            durationStr="1 D",
            barSizeSetting="1 min",
            whatToShow="TRADES",
            useRTH=False,
            formatDate=1,
            keepUpToDate=False,
        )

        self.df_1min = self._bars_to_df(bars_1min)
        return self

    def get_prev_3mo_history(self):
        bars_90d = ib.reqHistoricalData(
            self.contract,
            endDateTime="",
            durationStr="3 M",
            barSizeSetting="1 day",
            whatToShow="TRADES",
            useRTH=False,
            formatDate=1,
            keepUpToDate=False,
        )

        self.df_90days = self._bars_to_df(bars_90d)
        return self

    @staticmethod
    def _bars_to_df(bars) -> pd.DataFrame:
        data = {
            "date": [bar.date for bar in bars],
            "open": [bar.open for bar in bars],
            "high": [bar.high for bar in bars],
            "low": [bar.low for bar in bars],
            "close": [bar.close for bar in bars],
            "volume": [bar.volume for bar in bars],
        }
        df = pd.DataFrame(data)
        if not isinstance(df["date"].iloc[0], pd.Timestamp):
            df["date"] = pd.to_datetime(df["date"])

        df = df.sort_values(by="date")
        return df

    def _get_option_chain_definitions(self):
        self.option_chain_definitions = ib.reqSecDefOptParams(
            self.contract.symbol, "", self.contract.secType, self.contract.conId
        )
        if not self.option_chain_definitions:
            raise ValueError(f"No option chain definitions found for ticker {self.ticker}.")
        return self

    def get_relevant_option_chain_definition(self):
        self._get_option_chain_definitions()
        assert self.option_chain_definitions is not None, f"Could not get option chain for ticker {self.ticker}"
        self.option_chain_definition = next(c for c in self.option_chain_definitions if c.exchange == self.exchange)
        return self

    def get_atm_calendar(self):
        closest_strike = min(self.option_chain_definition.strikes, key=lambda x: abs(x - self.underlying_price))

        today = datetime.now()
        expiry_dates = [datetime.strptime(date_str, "%Y%m%d") for date_str in self.option_chain_definition.expirations]
        closest_expiry = min((d for d in expiry_dates if d > today), default=None)

        if closest_expiry:
            closest_expiry_plus_30days = next((d for d in expiry_dates if d >= (closest_expiry + timedelta(days=30))), None)
            if closest_expiry_plus_30days:
                expiry_plus_30_str = closest_expiry_plus_30days.strftime("%Y%m%d")
            else:
                raise ValueError(f"No expiry found 30+ days after closest for ticker {self.ticker}.")
        else:
            raise ValueError(f"No expiry found for closest expiry for ticker {self.ticker}.")

        call_near_exp = Contract(
            secType="OPT",
            symbol=self.ticker,
            exchange=self.exchange,
            currency=self.currency,
            lastTradeDateOrContractMonth=closest_expiry,
            strike=closest_strike,
            right="C",
        )
        put_near_exp = Contract(
            secType="OPT",
            symbol=self.ticker,
            exchange=self.exchange,
            currency=self.currency,
            lastTradeDateOrContractMonth=closest_expiry,
            strike=closest_strike,
            right="P",
        )

        call_exp_30days_out = Contract(
            secType="OPT",
            symbol=self.ticker,
            exchange=self.exchange,
            currency=self.currency,
            lastTradeDateOrContractMonth=expiry_plus_30_str,
            strike=closest_strike,
            right="C",
        )
        put_exp_30days_out = Contract(
            secType="OPT",
            symbol=self.ticker,
            exchange=self.exchange,
            currency=self.currency,
            lastTradeDateOrContractMonth=expiry_plus_30_str,
            strike=closest_strike,
            right="P",
        )

        return call_near_exp, put_near_exp, call_exp_30days_out, put_exp_30days_out

    @staticmethod
    def _filter_expirations(exp_dates):
        today = datetime.today().date()
        cutoff_date = today + timedelta(days=45)

        sorted_exp_dates = sorted(datetime.strptime(date, "%Y%m%d").date() for date in exp_dates)

        arr = []
        for i, date in enumerate(sorted_exp_dates):
            if date >= cutoff_date:
                arr = [d.strftime("%Y%m%d") for d in sorted_exp_dates[: i + 1]]
                break

        if len(arr) > 0:
            if arr[0] == today.strftime("%Y-%m-%d"):
                return arr[1:]
            return arr
        raise ValueError("No date 45 days or more in the future found.")

    def get_current_price(self):
        return self.df_1min["close"].iloc[-1]


class EarningsIVCrushScanner(MarketDataExtractor):
    @staticmethod
    def yang_zhang(price_data, window=30, trading_periods=252, return_last_only=True):
        log_ho = (price_data["high"] / price_data["open"]).apply(np.log)
        log_lo = (price_data["low"] / price_data["open"]).apply(np.log)
        log_co = (price_data["close"] / price_data["open"]).apply(np.log)

        log_oc = (price_data["open"] / price_data["close"].shift(1)).apply(np.log)
        log_oc_sq = log_oc**2

        log_cc = (price_data["close"] / price_data["close"].shift(1)).apply(np.log)
        log_cc_sq = log_cc**2

        rs = log_ho * (log_ho - log_co) + log_lo * (log_lo - log_co)

        close_vol = log_cc_sq.rolling(window=window, center=False).sum() * (1.0 / (window - 1.0))

        open_vol = log_oc_sq.rolling(window=window, center=False).sum() * (1.0 / (window - 1.0))

        window_rs = rs.rolling(window=window, center=False).sum() * (1.0 / (window - 1.0))

        k = 0.3333 / (1.3333 + ((window + 1) / (window - 1)))
        result = (open_vol + k * close_vol + (1 - k) * window_rs).apply(np.sqrt) * np.sqrt(trading_periods)

        if return_last_only:
            return result.iloc[-1]
        else:
            return result.dropna()

    @staticmethod
    def build_term_structure(days, ivs):
        days = np.array(days)
        ivs = np.array(ivs)

        sort_idx = days.argsort()
        days = days[sort_idx]
        ivs = ivs[sort_idx]

        spline = interp1d(days, ivs, kind="linear", fill_value="extrapolate")

        def term_spline(dte):
            if dte < days[0]:
                return ivs[0]
            elif dte > days[-1]:
                return ivs[-1]
            else:
                return float(spline(dte))

        return term_spline

    @staticmethod
    def calc_kelly_bet(
        p_win: float = 0.66,
        odds_decimal: float = 1.66,
        current_bankroll: float = 10000,
        pct_kelly=0.10,
    ) -> float:
        """
        Calculates the Kelly Criterion optimal bet amount.

        The Kelly Criterion is a formula used to determine the optimal size of a series
        of bets to maximize the long-term growth rate of a bankroll.

        Args:
            p_win: The estimated probability of winning the bet (p),
                                    a float between 0 and 1.
            odds_decimal: The decimal odds (b), where a successful $1 bet returns $b.
                          For example, if odds are 2:1, odds_decimal is 3.0.
                          If odds are 1:1, odds_decimal is 2.0.
                          This is (payout / stake) + 1.
            current_bankroll: The total amount of money available to bet (B).

        Returns:
            The calculated optimal bet amount. Returns 0 if the bet is not favorable
            (i.e., the calculated fraction is negative or zero), or if inputs are invalid.
        """
        if not (0 <= p_win <= 1):
            raise ValueError("Probability of winning must be between 0 and 1.")
        if odds_decimal <= 1.0:  # Odds must be greater than 1.0 (e.g., 1.01 for a tiny profit)
            raise ValueError("Decimal odds must be greater than 1.0 (e.g., 1.01 for a winning bet).")
        if current_bankroll <= 0:
            raise ValueError("Current bankroll must be a positive number.")

        b_kelly = odds_decimal - 1.0

        if b_kelly <= 0:  # Should be caught by odds_decimal check, but as a safeguard
            return 0.0

        kelly_fraction = p_win - ((1 - p_win) / b_kelly)

        if kelly_fraction <= 0:
            return 0.0

        bet_amount = kelly_fraction * current_bankroll
        bet_amount = bet_amount * pct_kelly
        return round(bet_amount, 2)

    def get_option_chain(self):
        # ticker = ib.reqMktData(self.contract, "", True)
        # ib.sleep(1)
        return self

    def compute_recommendation(self, ticker: str, min_avg_30d_volume=1500000, min_iv30_rv30=2.0, max_ts_slope_0_45=-0.0075):
        try:
            ticker = ticker.upper()
            if not ticker:
                return "No stock symbol provided."

            self.get_relevant_option_chain_definition()
            assert self.option_chain_definitions is not None, f"Could not get option chain for ticker {ticker}"

            self.get_prev_3mo_history()
            self.get_1min_bars()

            try:
                self.underlying_price = self.get_current_price()
                if self.underlying_price is None:
                    raise ValueError("No market price found.")
            except Exception as e:
                raise Exception(f"Error: Unable to retrieve underlying stock price. {e}")

            atm_iv = {}
            self.get_atm_calendar()

            exp_dates = self.option_chain_definition.expirations
            filtered_expirations = self._filter_expirations(exp_dates)

            self.get_option_chain()  # TODO: Stuck here pulling options data --> May need subscription even for delayed data?
            for exp_date in filtered_expirations:
                print(exp_date)
            #     calls = self.option_chain.calls
            #     puts = chain.puts
            #
            #     if calls.empty or puts.empty:
            #         continue
            #
            #     call_diffs = (calls["strike"] - underlying_price).abs()
            #     call_idx = call_diffs.idxmin()
            #     call_iv = calls.loc[call_idx, "impliedVolatility"]
            #
            #     put_diffs = (puts["strike"] - underlying_price).abs()
            #     put_idx = put_diffs.idxmin()
            #     put_iv = puts.loc[put_idx, "impliedVolatility"]
            #
            #     atm_iv_value = (call_iv + put_iv) / 2.0
            #     atm_iv[exp_date] = atm_iv_value
            #
            #     if i == 0:
            #         call_bid = calls.loc[call_idx, "bid"]
            #         call_ask = calls.loc[call_idx, "ask"]
            #         put_bid = puts.loc[put_idx, "bid"]
            #         put_ask = puts.loc[put_idx, "ask"]
            #
            #         if call_bid is not None and call_ask is not None:
            #             call_mid = (call_bid + call_ask) / 2.0
            #         else:
            #             call_mid = None
            #
            #         if put_bid is not None and put_ask is not None:
            #             put_mid = (put_bid + put_ask) / 2.0
            #         else:
            #             put_mid = None
            #
            #         if call_mid is not None and put_mid is not None:
            #             pass  # straddle = call_mid + put_mid
            #
            #     i += 1

            if not atm_iv:
                return "Error: Could not determine ATM IV for any expiration dates."

            today = datetime.today().date()
            dtes = []
            ivs = []
            for exp_date, iv in atm_iv.items():
                exp_date_obj = datetime.strptime(exp_date, "%Y-%m-%d").date()
                days_to_expiry = (exp_date_obj - today).days
                dtes.append(days_to_expiry)
                ivs.append(iv)

            # term_spline = self.build_term_structure(dtes, ivs)

            # ts_slope_0_45 = (term_spline(45) - term_spline(dtes[0])) / (45 - dtes[0])

            # iv30_rv30 = term_spline(30) / self.yang_zhang(df_price_history)
            #
            # avg_volume = df_price_history["Volume"].rolling(30).mean().dropna().iloc[-1]

            # expected_move = str(round(straddle / underlying_price * 100, 2)) + "%" if straddle else None

            # result_summary = {
            #     "avg_volume": round(avg_volume, 3),
            #     "avg_volume_pass": avg_volume >= min_avg_30d_volume,
            #     "iv30_rv30": round(iv30_rv30, 3),
            #     "iv30_rv30_pass": iv30_rv30 >= min_iv30_rv30,
            #     "ts_slope_0_45": ts_slope_0_45,
            #     "ts_slope_0_45_pass": ts_slope_0_45 <= max_ts_slope_0_45,
            #     "expected_move": expected_move,
            # }

            # if result_summary["avg_volume_pass"]
            # and result_summary["iv30_rv30_pass"] and result_summary["ts_slope_0_45_pass"]:
            #     suggestion = "Recommended"
            # elif result_summary["ts_slope_0_45_pass"] and (
            #     (result_summary["avg_volume_pass"] and not result_summary["iv30_rv30_pass"])
            #     or (result_summary["iv30_rv30_pass"] and not result_summary["avg_volume_pass"])
            # ):
            #     suggestion = "Consider"
            # else:
            #     suggestion = "Avoid"

            # result_summary["suggestion"] = suggestion

            # if suggestion == "Recommended":
            #     kelly_bet = self.calc_kelly_bet()
            # elif suggestion == "Consider":
            #     kelly_bet = self.calc_kelly_bet(p_win=0.33)
            # else:
            #     kelly_bet = 0
            #
            # result_summary["kelly_bet"] = kelly_bet

            # Check that they are in our desired range (see video)
            return {}  # result_summary

        except Exception as e:
            raise Exception(f"Error occured processing - {e}")


if __name__ == "__main__":
    ib = IB()

    # ib.connect('127.0.0.1', 7497, clientId=1)  # TWS
    # ib.connect('127.0.0.1', 4001, clientId=1)  # Gateway production
    ib.connect("127.0.0.1", 4002, clientId=2)  # Gateway paper-trading

    # data_getter = MarketDataExtractor(ticker="AAPL", exchange="SMART", currency="USD")

    scanner = EarningsIVCrushScanner(ticker="AAPL", exchange="SMART", currency="USD")
    scanner.compute_recommendation(
        ticker="AAPL",
        min_avg_30d_volume=1500000,
        min_iv30_rv30=2.0,
        max_ts_slope_0_45=-0.0075,
    )

    parser = argparse.ArgumentParser(description="Earnings IV Crush Scanner")
    parser.add_argument(
        "--tickers",
        nargs="+",
        required=True,
        help="List of ticker symbols (e.g., NVDA AAPL TSLA). If set to * then pull all tickers that have earnings for today.",
    )

    args = parser.parse_args()
    tickers = args.tickers

    ib.disconnect()
