import ib_async as ib
from datetime import datetime, timedelta
from scipy.interpolate import interp1d
import numpy as np
import argparse
import warnings
import pandas as pd
import os
import requests
from dateutil.relativedelta import relativedelta
import yfinance as yf
import yaml

import logging

warnings.filterwarnings("ignore", message="Not enough unique days to interpolate for ticker")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "config.yml")
    with open(config_path, "r") as file:
        return yaml.safe_load(file)


config = load_config()


### Assumptions ###
#   - Win rate: 66%
#   - Expectancy per trade: 0.265% ---> this implies kelly odds decimal is 1.5578
#   - Gains: Assume average gain is 65% of the debit (not stated in the video)
#   - Losses: Full debit + ~7.5% in commissions/slippage (estimated from the video)
#   => Kelly Odds Decimal: ~1.65 (estimated)
#   => Expectancy: ~6.35% per trade
#   => Position sizing: ~1.37% of account per trade


### ------ Configuration-based Globals ------ ###

DEBUG = config["settings"]["debug"]

MIN_AVG_30D_DOLLAR_VOLUME = config["screening"]["min_avg_30d_dollar_volume"]
MIN_AVG_30D_SHARE_VOLUME = config["screening"]["min_avg_30d_share_volume"]
MIN_IV30_RV30 = config["screening"]["min_iv30_rv30"]
MAX_TS_SLOPE_0_45 = config["screening"]["max_ts_slope_0_45"]
MIN_SHARE_PRICE = config["screening"]["min_share_price"]
EARNINGS_LOOKBACK_DAYS_FOR_AGG = config["screening"]["earnings_lookback_days"]
MAX_KELLY_BET = config["kelly"]["max_bet"]

KELLY_WIN_RATE = config["kelly"]["win_rate"]
KELLY_ODDS_DECIMAL = config["kelly"]["odds_decimal"]
KELLY_FRACTIONAL = config["kelly"]["fractional"]
KELLY_BANKROLL = config["kelly"]["bankroll"]

PLOT_LOC = config["settings"]["plot_location"]

### Estimated higher probabilities of success by assigning tiers ###

TIER_1_AVG_30D_DOLLAR_VOLUME = config["tier_1"]["min_avg_30d_dollar_volume"]
TIER_1_IV30_RV30 = config["tier_1"]["min_iv30_rv30"]
TIER_1_TS_SLOPE_0_45 = config["tier_1"]["max_ts_slope_0_45"]
MIN_SHARE_PRICE_TIER_1 = config["tier_1"]["min_share_price"]
TIER_1_MAX_SPREAD_PCT = config["tier_1"]["max_spread_pct"]

TIER_2_AVG_30D_DOLLAR_VOLUME = config["tier_2"]["min_avg_30d_dollar_volume"]
TIER_2_IV30_RV30 = config["tier_2"]["min_iv30_rv30"]
TIER_2_TS_SLOPE_0_45 = config["tier_2"]["max_ts_slope_0_45"]
MIN_SHARE_PRICE_TIER_2 = config["tier_2"]["min_share_price"]
TIER_2_MAX_SPREAD_PCT = config["tier_2"]["max_spread_pct"]

TIER_3_AVG_30D_DOLLAR_VOLUME = config["tier_3"]["min_avg_30d_dollar_volume"]
TIER_3_IV30_RV30 = config["tier_3"]["min_iv30_rv30"]
TIER_3_TS_SLOPE_0_45 = config["tier_3"]["max_ts_slope_0_45"]
MIN_SHARE_PRICE_TIER_3 = config["tier_3"]["min_share_price"]
TIER_3_MAX_SPREAD_PCT = config["tier_3"]["max_spread_pct"]


# Interactive Brokers connection settings
IB_HOST = config["ib"]["host"]
IB_PORT = config["ib"]["port"]
IB_CLIENT_ID = config["ib"]["client_id"]


# Market Data Configuration
MARKET_DATA_TYPE = config["market_data"]["type"]
DEVELOPMENT_MODE = config["market_data"]["development_mode"]


# Current Subscriptions Required (Total: $6.00/month):
# 1. NYSE (Network A/CTA) (NP,L1) - $1.50/month
# 2. NASDAQ (Network C/UTP) (NP,L1) - $1.50/month
# 3. NYSE American, BATS, ARCA, IEX (Network B) (NP,L1) - $1.50/month
# 4. OPRA (US Options Exchanges) (NP,L1) - $1.50/month (waived with $20+ commissions)


class MarketDataExtractor:
    def __init__(self, ticker: str):
        self.ticker = ticker
        self.stock_contract = ib.Stock(self.ticker, "SMART", "USD")
        ib_client.qualifyContracts(self.stock_contract)

        self.chain = None
        self.option_qualified_contracts = None
        self.valid_expirations = None
        self.bars_1min = None
        self.bars_3mo = None
        self.bars_3y = None

    def get_historical_bars_1min(self, duration_str="1 D", bar_size="1 min", use_rth=False):
        # Note: For this strategy we don't need real-time bars. Historical 1-minute bars are ok. For lower latency
        # strategies, we need to subscribe to real-time bars (say 1 second), then call
        # ib_client.reqRealTimeBars(contract, 1, "MIDPOINT", False)

        self.bars_1min = ib_client.reqHistoricalData(
            self.stock_contract,
            endDateTime="",
            durationStr=duration_str,
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=use_rth,  # Include extended hours for intraday, RTH for daily
            formatDate=1,
        )
        return self

    def get_historical_bars_3y(self, duration_str="3 Y", bar_size="1 day", use_rth=False, convert_to_df=True):
        self.bars_3y = ib_client.reqHistoricalData(
            self.stock_contract,
            endDateTime="",
            durationStr=duration_str,
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=use_rth,  # Include extended hours for intraday, RTH for daily
            formatDate=1,
        )
        if convert_to_df and not isinstance(self.bars_3y, pd.DataFrame):
            self.bars_3y = ib.util.df(self.bars_3y)
            self.bars_3y["date"] = pd.to_datetime(self.bars_3y["date"])

    def get_historical_bars_3mo(self):
        if not self.bars_3y:
            self.get_historical_bars_3y()

        cutoff_date = pd.Timestamp.now() - relativedelta(months=3)
        self.bars_3mo = self.bars_3y[self.bars_3y["date"] >= cutoff_date].copy()
        return self

    def get_underlying_last_price(self):
        if not self.bars_1min:
            self.get_historical_bars_1min()

        if self.bars_1min:
            current_price = self.bars_1min[-1].close
            logging.info(f"Using recent price for {self.ticker}: ${current_price:.2f}")
        else:
            raise ValueError(f"Could not get current price for ticker {self.ticker}")
        return current_price

    def get_option_chain(self, exchange="SMART"):
        if not self.chain:
            chains = ib_client.reqSecDefOptParams(
                self.stock_contract.symbol, "", self.stock_contract.secType, self.stock_contract.conId
            )
            self.chain = next(c for c in chains if c.tradingClass == self.ticker and c.exchange == exchange)
        return self

    def get_valid_expirations(self, max_days_out=75):
        today = datetime.today().date()
        max_date = today + timedelta(days=max_days_out)
        min_calendar_date = today + timedelta(days=45)

        self.valid_expirations = []
        has_45_day_expiration = False

        for expiration in sorted(self.chain.expirations):
            exp_date_obj = datetime.strptime(expiration, "%Y%m%d").date()
            if today < exp_date_obj <= max_date:
                self.valid_expirations.append(expiration)
                if exp_date_obj >= min_calendar_date:
                    has_45_day_expiration = True

        if not has_45_day_expiration:
            raise ValueError("No expiration date 45 days or more in the future found. Calendar spread strategy not viable.")

        logging.info(f"Processing {len(self.valid_expirations)} expirations within 75 days for {self.ticker}")
        self.valid_expirations = sorted(self.valid_expirations)
        return self

    def get_option_qualified_contracts(self, strike_range=(0.85, 1.15)):
        self.get_option_chain()
        underlying_price = self.get_underlying_last_price()
        self.strikes = [
            strike
            for strike in self.chain.strikes
            if underlying_price * strike_range[0] < strike < underlying_price * strike_range[1]
        ]

        if not self.valid_expirations:
            self.get_valid_expirations()

        if DEBUG:
            self.valid_expirations = self.valid_expirations[0:3]
            closest_strike = min(self.strikes, key=lambda x: abs(x - underlying_price))
            closest_idx = self.strikes.index(closest_strike)
            start_idx = max(0, closest_idx - 1)
            end_idx = min(len(self.strikes), closest_idx + 2)
            self.strikes = self.strikes[start_idx:end_idx]

        rights = ["P", "C"]

        contracts = [
            ib.Option(self.ticker, expiration, strike, right, "SMART")
            for right in rights
            for expiration in self.valid_expirations
            for strike in self.strikes
        ]

        try:
            self.option_qualified_contracts = ib_client.qualifyContracts(*contracts)
            if not self.option_qualified_contracts:
                logging.error(f"No qualified option contracts found for {self.ticker}")
                return []
            logging.info(
                f"Qualified {len(self.option_qualified_contracts)} out of {len(contracts)} option contracts for {self.ticker}"
            )
            return self
        except Exception as e:
            logging.error(f"Error qualifying option contracts for {self.ticker}: {e}")
            return []

    def calc_option_greeks(self, option_type, strike, exp):
        """Get Greeks and market data for puts or calls - single API call"""
        current_price = self.get_underlying_last_price()
        option = ib.Option(self.ticker, exp, strike, option_type, "SMART")
        qualified_option = ib_client.qualifyContracts(option)[0]

        ticker = ib_client.reqTickers(qualified_option)[0]

        # Extract all market data from the ticker
        market_data = {
            "contract": qualified_option,
            "bid": ticker.bid if ticker.bid and ticker.bid > 0 else None,
            "ask": ticker.ask if ticker.ask and ticker.ask > 0 else None,
            "last": ticker.last if ticker.last and ticker.last > 0 else None,
            "market_price": ticker.marketPrice() if ticker.marketPrice() and ticker.marketPrice() > 0 else None,
            "volume": getattr(ticker, "volume", None),
            "open_interest": getattr(ticker, "openInterest", None),
            "bid_size": getattr(ticker, "bidSize", None),
            "ask_size": getattr(ticker, "askSize", None),
        }

        # Calculate spread metrics
        if market_data["bid"] and market_data["ask"]:
            market_data["spread"] = market_data["ask"] - market_data["bid"]
            market_data["mid_price"] = (market_data["bid"] + market_data["ask"]) / 2.0
            market_data["spread_pct"] = (
                market_data["spread"] / market_data["mid_price"] if market_data["mid_price"] > 0 else None
            )
        else:
            market_data["spread"] = None
            market_data["mid_price"] = None
            market_data["spread_pct"] = None

        logging.info(
            f"Ticker data for {self.ticker} {strike} {option_type}: "
            f"marketPrice={market_data['market_price']}, last={market_data['last']}, "
            f"bid={market_data['bid']}, ask={market_data['ask']}"
        )

        # Determine option price for Greeks calculation
        if market_data["market_price"]:
            option_price = market_data["market_price"]
        elif market_data["last"]:
            option_price = market_data["last"]
        elif market_data["mid_price"]:
            option_price = market_data["mid_price"]
        else:
            logging.warning("Option real-time data unavailable - falling back to historical bars.")
            option_bars = ib_client.reqHistoricalData(qualified_option, "", "1 D", "1 min", "TRADES", True)
            if not option_bars:
                option_bars = ib_client.reqHistoricalData(qualified_option, "", "1 D", "1 min", "MIDPOINT", True)

            if not option_bars:
                if option_type == "C":  # Call
                    intrinsic_value = max(0, current_price - strike)
                else:  # Put
                    intrinsic_value = max(0, strike - current_price)

                # For ITM options, use intrinsic + small time value
                if intrinsic_value > 0.01:
                    option_price = intrinsic_value + 0.01  # Add minimal time value
                    logging.warning(
                        f"No market data for {self.ticker} {strike} {option_type}, "
                        f"using intrinsic value ${intrinsic_value:.2f} + $0.01"
                    )
                else:
                    # For OTM options expiring today, use minimal time value
                    option_price = 0.01  # Minimal option price
                    logging.warning(
                        f"No market data for {self.ticker} {strike} {option_type}, "
                        f"using minimal time value $0.01 (intrinsic=${intrinsic_value:.2f})"
                    )
            else:
                option_price = option_bars[-1].close

        market_data["calculated_price"] = option_price

        # Calculate Greeks
        try:
            iv_result = ib_client.calculateImpliedVolatility(qualified_option, option_price, current_price)
            if iv_result and hasattr(iv_result, "impliedVol") and iv_result.impliedVol > 0:
                iv = iv_result.impliedVol
            else:
                logging.error(f"Invalid IV result for {self.ticker} {strike} {option_type}. Setting IV to HV.")
                if not hasattr(self, "_hv30_cache"):
                    if self.bars_3mo is None or self.bars_3mo.empty:
                        self.get_historical_bars_3mo()
                    self._hv30_cache = yang_zhang(self.bars_3mo)
                iv = self._hv30_cache

            greeks = ib_client.calculateOptionPrice(qualified_option, iv, current_price)
            market_data["greeks"] = greeks
            market_data["iv"] = iv
        except Exception as e:
            logging.error(f"Error calculating Greeks for {self.ticker} {strike} {option_type}: {e}")
            market_data["greeks"] = None
            market_data["iv"] = None

        return market_data

    def get_option_data(self, expiration, strike):
        """Get comprehensive option data for a specific expiration and strike combination"""

        try:
            put_data = self.calc_option_greeks("P", strike, expiration)
        except Exception as e:
            put_data = None
            logging.warning(f"No put options data found for ticker {self.ticker} and strike {strike}. Skipping... {e}")

        try:
            call_data = self.calc_option_greeks("C", strike, expiration)
        except Exception as e:
            call_data = None
            logging.warning(f"No call options data found for ticker {self.ticker} and strike {strike}. Skipping... {e}")

        if put_data and put_data.get("greeks"):
            if put_data.get("bid") and put_data.get("ask"):
                put_info = (
                    f"Bid/Ask: ${put_data['bid']:.2f}/${put_data['ask']:.2f}, "
                    f"Spread: ${put_data['spread']:.2f} ({put_data['spread_pct'] * 100:.1f}%), "
                    f"Vol: {put_data['volume']}, OI: {put_data['open_interest']}, "
                    f"Δ={put_data['greeks'].delta:.3f}, Γ={put_data['greeks'].gamma:.4f}, "
                    f"Θ={put_data['greeks'].theta:.3f}, ν={put_data['greeks'].vega:.3f}, "
                    f"IV={put_data['iv']:.3f}"
                )
            else:
                put_info = (
                    f"Δ={put_data['greeks'].delta:.3f}, Γ={put_data['greeks'].gamma:.4f}, "
                    f"Θ={put_data['greeks'].theta:.3f}, ν={put_data['greeks'].vega:.3f}, "
                    f"IV={put_data['iv']:.3f} (Limited market data)"
                )
            logging.info(f"Strike {strike}: Put[{put_info}]")

        if call_data and call_data.get("greeks"):
            if call_data.get("bid") and call_data.get("ask"):
                call_info = (
                    f"Bid/Ask: ${call_data['bid']:.2f}/${call_data['ask']:.2f}, "
                    f"Spread: ${call_data['spread']:.2f} ({call_data['spread_pct'] * 100:.1f}%), "
                    f"Vol: {call_data['volume']}, OI: {call_data['open_interest']}, "
                    f"Δ={call_data['greeks'].delta:.3f}, Γ={call_data['greeks'].gamma:.4f}, "
                    f"Θ={call_data['greeks'].theta:.3f}, ν={call_data['greeks'].vega:.3f}, "
                    f"IV={call_data['iv']:.3f}"
                )
            else:
                call_info = (
                    f"Δ={call_data['greeks'].delta:.3f}, Γ={call_data['greeks'].gamma:.4f}, "
                    f"Θ={call_data['greeks'].theta:.3f}, ν={call_data['greeks'].vega:.3f}, "
                    f"IV={call_data['iv']:.3f} (Limited market data)"
                )
            logging.info(f"Strike {strike} & Expiration {expiration}: Call[{call_info}]")

        return put_data, call_data

    def get_all_option_data(self):
        """Process ALL option data using batch API calls - MUCH faster for production"""
        if not self.chain:
            self.get_option_chain()
        if not self.valid_expirations:
            self.get_valid_expirations()
        if not hasattr(self, "strikes") or not self.strikes:
            self.get_option_qualified_contracts()

        all_contracts = []
        contract_map = {}

        for exp in self.valid_expirations:
            for strike in self.strikes:
                for option_type in ["P", "C"]:
                    contract = ib.Option(self.ticker, exp, strike, option_type, "SMART")
                    all_contracts.append(contract)
                    contract_map[id(contract)] = (exp, strike, option_type)

        logging.info(f"Batch processing {len(all_contracts)} option contracts for {self.ticker}")

        try:
            qualified_contracts = ib_client.qualifyContracts(*all_contracts)
            if not qualified_contracts:
                logging.error(f"No qualified contracts returned for {self.ticker}")
                return []
            logging.info(f"Successfully qualified {len(qualified_contracts)} contracts")
        except Exception as e:
            logging.error(f"Batch contract qualification failed for {self.ticker}: {e}")
            return []

        try:
            all_tickers = ib_client.reqTickers(*qualified_contracts)
            if not all_tickers:
                logging.error(f"No ticker data returned for {self.ticker}")
                return []
            logging.info(f"Successfully retrieved {len(all_tickers)} ticker data")
        except Exception as e:
            logging.error(f"Batch ticker request failed for {self.ticker}: {e}")
            return []

        results = {}
        current_price = self.get_underlying_last_price()

        # Cache HV calculation
        if not hasattr(self, "_hv30_cache"):
            if self.bars_3mo is None or self.bars_3mo.empty:
                self.get_historical_bars_3mo()
            self._hv30_cache = yang_zhang(self.bars_3mo)

        for i, (contract, ticker) in enumerate(zip(qualified_contracts, all_tickers)):
            try:
                # Get contract details from our map
                contract_id = id(all_contracts[i])
                exp, strike, option_type = contract_map[contract_id]

                # Extract market data
                market_data = {
                    "contract": contract,
                    "bid": ticker.bid if ticker.bid and ticker.bid > 0 else None,
                    "ask": ticker.ask if ticker.ask and ticker.ask > 0 else None,
                    "last": ticker.last if ticker.last and ticker.last > 0 else None,
                    "market_price": ticker.marketPrice() if ticker.marketPrice() and ticker.marketPrice() > 0 else None,
                    "volume": getattr(ticker, "volume", None),
                    "open_interest": getattr(ticker, "openInterest", None),
                    "bid_size": getattr(ticker, "bidSize", None),
                    "ask_size": getattr(ticker, "askSize", None),
                }

                # Calculate spread metrics
                if market_data["bid"] and market_data["ask"]:
                    market_data["spread"] = market_data["ask"] - market_data["bid"]
                    market_data["mid_price"] = (market_data["bid"] + market_data["ask"]) / 2.0
                    market_data["spread_pct"] = (
                        market_data["spread"] / market_data["mid_price"] if market_data["mid_price"] > 0 else None
                    )
                else:
                    market_data["spread"] = None
                    market_data["mid_price"] = None
                    market_data["spread_pct"] = None

                # Determine option price for Greeks
                if market_data["market_price"]:
                    option_price = market_data["market_price"]
                elif market_data["last"]:
                    option_price = market_data["last"]
                elif market_data["mid_price"]:
                    option_price = market_data["mid_price"]
                else:
                    # Use intrinsic value fallback
                    if option_type == "C":
                        intrinsic_value = max(0, current_price - strike)
                    else:
                        intrinsic_value = max(0, strike - current_price)
                    option_price = max(0.01, intrinsic_value + 0.01)

                market_data["calculated_price"] = option_price

                # Calculate Greeks (still individual calls, but with cached HV)
                try:
                    iv_result = ib_client.calculateImpliedVolatility(contract, option_price, current_price)
                    if iv_result and hasattr(iv_result, "impliedVol") and iv_result.impliedVol > 0:
                        iv = iv_result.impliedVol
                    else:
                        iv = self._hv30_cache

                    greeks = ib_client.calculateOptionPrice(contract, iv, current_price)
                    market_data["greeks"] = greeks
                    market_data["iv"] = iv
                except Exception as e:
                    logging.warning(f"Greeks calculation failed for {self.ticker} {strike} {option_type}: {e}")
                    market_data["greeks"] = None
                    market_data["iv"] = None

                # Store in results structure
                key = (exp, strike)
                if key not in results:
                    results[key] = {"expiration": exp, "strike": strike}

                if option_type == "P":
                    results[key]["put_data"] = market_data
                else:
                    results[key]["call_data"] = market_data

            except Exception as e:
                logging.error(f"Error processing contract {i} for {self.ticker}: {e}")
                continue

        # Convert to list format
        final_results = list(results.values())
        logging.info(f"Batch processing complete: {len(final_results)} strike/expiration combinations")
        return final_results

    def _parse_option_chain(self, option_results):
        """Convert IB option results into yfinance-compatible options_chains format"""
        from collections import namedtuple

        # Create a named tuple to mimic yfinance chain structure
        OptionChain = namedtuple("OptionChain", ["calls", "puts"])
        options_chains = {}

        # Group by expiration
        exp_groups = {}
        for result in option_results:
            exp = result["expiration"]
            # Convert IB expiration format (YYYYMMDD) to yfinance format (YYYY-MM-DD)
            if len(exp) == 8:
                exp_key = f"{exp[:4]}-{exp[4:6]}-{exp[6:8]}"
            else:
                exp_key = exp

            if exp_key not in exp_groups:
                exp_groups[exp_key] = {"calls": [], "puts": []}

            # Add call data if available
            if result.get("call_data") and result["call_data"].get("iv"):
                call_row = {
                    "strike": result["strike"],
                    "bid": result["call_data"].get("bid") or 0,
                    "ask": result["call_data"].get("ask") or 0,
                    "lastPrice": result["call_data"].get("last") or 0,
                    "impliedVolatility": result["call_data"].get("iv") or 0,
                    "volume": result["call_data"].get("volume") or 0,
                    "openInterest": result["call_data"].get("open_interest") or 0,
                }
                exp_groups[exp_key]["calls"].append(call_row)

            # Add put data if available
            if result.get("put_data") and result["put_data"].get("iv"):
                put_row = {
                    "strike": result["strike"],
                    "bid": result["put_data"].get("bid") or 0,
                    "ask": result["put_data"].get("ask") or 0,
                    "lastPrice": result["put_data"].get("last") or 0,
                    "impliedVolatility": result["put_data"].get("iv") or 0,
                    "volume": result["put_data"].get("volume") or 0,
                    "openInterest": result["put_data"].get("open_interest") or 0,
                }
                exp_groups[exp_key]["puts"].append(put_row)

        # Convert lists to DataFrames and create namedtuple structure
        for exp_key in exp_groups:
            calls_df = pd.DataFrame(exp_groups[exp_key]["calls"]) if exp_groups[exp_key]["calls"] else pd.DataFrame()
            puts_df = pd.DataFrame(exp_groups[exp_key]["puts"]) if exp_groups[exp_key]["puts"] else pd.DataFrame()
            options_chains[exp_key] = OptionChain(calls=calls_df, puts=puts_df)

        return options_chains

    @staticmethod
    def _filter_option_exp_dates(dates):
        today = datetime.today().date()
        cutoff_date = today + timedelta(days=45)

        sorted_dates = sorted(datetime.strptime(date, "%Y-%m-%d").date() for date in dates)

        arr = []
        for i, date in enumerate(sorted_dates):
            if date >= cutoff_date:
                arr = [d.strftime("%Y-%m-%d") for d in sorted_dates[: i + 1]]
                break

        if len(arr) > 0:
            if arr[0] == today.strftime("%Y-%m-%d"):
                return arr[1:]
            return arr

        raise ValueError("No date 45 days or more in the future found.")

    def get_option_exp_dates(self):
        all_exp_dates = None
        try:
            stock = yf.Ticker(self.ticker)
            n_tries = 3
            i = 0
            while i < n_tries:
                all_exp_dates = list(stock.options)
                if all_exp_dates:
                    break
                i += 1
            if len(all_exp_dates) == 0:
                raise KeyError(f"No options data found for ticker {ticker}")
        except KeyError:
            return f"Error: No options found for stock symbol '{ticker}'."

        try:
            filtered_exp_dates = self._filter_option_exp_dates(all_exp_dates)
            return filtered_exp_dates
        except Exception as e:
            raise f"Error: Not enough option data. {e}"


class EarningsIVScanner(MarketDataExtractor):
    """Earnings IV Crush Scanner that inherits market data capabilities"""

    def __init__(self, ticker: str):
        super().__init__(ticker)

    def compute_recommendation(
        self,
        ticker,
        min_avg_30d_dollar_volume=MIN_AVG_30D_DOLLAR_VOLUME,
        min_avg_30d_share_volume=MIN_AVG_30D_SHARE_VOLUME,
        min_iv30_rv30=MIN_IV30_RV30,
        max_ts_slope_0_45=MAX_TS_SLOPE_0_45,
        plot_loc=PLOT_LOC,
    ):
        ticker = ticker.strip().upper()
        if not ticker:
            return "No stock symbol provided."

        option_results = self.get_all_option_data()
        options_chains = self._parse_option_chain(option_results)

        if self.bars_3mo is None or self.bars_3mo.empty:
            self.get_historical_bars_3mo()

        df_price_history_3mo = self.bars_3mo[self.bars_3mo["date"] >= (pd.Timestamp.now() - relativedelta(months=3))]
        df_price_history_3mo = df_price_history_3mo.sort_values(by="date")
        df_price_history_3mo["dollar_volume"] = df_price_history_3mo["volume"] * df_price_history_3mo["close"]

        try:
            underlying_price = self.get_underlying_last_price()
            if underlying_price is None:
                raise ValueError("No market price found.")
        except Exception as e:
            return f"Error: Unable to retrieve underlying stock price. {e}"

        atm_iv = {}
        atm_call_iv = {}
        atm_put_iv = {}
        straddle = None
        straddle_call_strike = None
        straddle_put_strike = None
        i = 0
        for exp_date, chain in options_chains.items():
            calls = chain.calls
            puts = chain.puts

            if calls is None or puts is None or calls.empty or puts.empty:
                continue

            call_diffs = (calls["strike"] - underlying_price).abs()
            call_idx = call_diffs.idxmin()
            call_iv = calls.loc[call_idx, "impliedVolatility"]

            put_diffs = (puts["strike"] - underlying_price).abs()
            put_idx = put_diffs.idxmin()
            put_iv = puts.loc[put_idx, "impliedVolatility"]

            atm_iv_value = (call_iv + put_iv) / 2.0
            atm_iv[exp_date] = atm_iv_value
            atm_call_iv[exp_date] = call_iv
            atm_put_iv[exp_date] = put_iv

            if i == 0:
                # Store the strike prices for the nearest expiration
                straddle_call_strike = calls.loc[call_idx, "strike"]
                straddle_put_strike = puts.loc[put_idx, "strike"]
                call_bid = calls.loc[call_idx, "bid"]
                call_ask = calls.loc[call_idx, "ask"]
                put_bid = puts.loc[put_idx, "bid"]
                put_ask = puts.loc[put_idx, "ask"]

                if call_bid is not None and call_ask is not None:
                    call_mid = (call_bid + call_ask) / 2.0
                else:
                    call_mid = None

                if put_bid is not None and put_ask is not None:
                    put_mid = (put_bid + put_ask) / 2.0
                else:
                    put_mid = None

                if call_mid is not None and put_mid is not None and call_mid != 0 and put_mid != 0:
                    straddle = call_mid + put_mid

                    call_spread = call_ask - call_bid
                    put_spread = put_ask - put_bid
                    total_straddle_spread = call_spread + put_spread
                    straddle_spread_pct = total_straddle_spread / underlying_price

                else:
                    straddle_spread_pct = 1.0  # Set high spread to fail all tier checks
                    try:
                        if call_idx + 1 < len(calls) and put_idx + 1 < len(puts):
                            warnings.warn(
                                f"For ticker {ticker} straddle is either 0 or None from "
                                f"available bid/ask spread... using nearest term strikes."
                            )
                            straddle = calls.iloc[call_idx + 1]["lastPrice"] + puts.iloc[put_idx + 1]["lastPrice"]
                        if not straddle:
                            warnings.warn(
                                f"For ticker {ticker} straddle is either 0 or None from "
                                f"available bid/ask spread... using lastPrice."
                            )
                            straddle = calls.iloc[call_idx]["lastPrice"] + puts.iloc[call_idx]["lastPrice"]
                    except IndexError:
                        warnings.warn(f"For ticker {ticker}, call_idx {call_idx} is out of bounds in calls/puts.")
                        return None
            i += 1

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

        term_spline = build_term_structure(dtes, ivs)
        if not term_spline:
            return

        # Build separate term structures for calls and puts
        call_dtes = []
        call_ivs = []
        put_dtes = []
        put_ivs = []
        for exp_date in atm_call_iv.keys():
            exp_date_obj = datetime.strptime(exp_date, "%Y-%m-%d").date()
            days_to_expiry = (exp_date_obj - today).days
            call_dtes.append(days_to_expiry)
            call_ivs.append(atm_call_iv[exp_date])
            put_dtes.append(days_to_expiry)
            put_ivs.append(atm_put_iv[exp_date])

        call_term_spline = build_term_structure(call_dtes, call_ivs)
        put_term_spline = build_term_structure(put_dtes, put_ivs)

        # Calculate term structure slopes
        ts_slope_0_45 = (term_spline(45) - term_spline(dtes[0])) / (45 - dtes[0])
        ts_slope_0_45_call = (
            (call_term_spline(45) - call_term_spline(call_dtes[0])) / (45 - call_dtes[0]) if call_term_spline else ts_slope_0_45
        )
        ts_slope_0_45_put = (
            (put_term_spline(45) - put_term_spline(put_dtes[0])) / (45 - put_dtes[0]) if put_term_spline else ts_slope_0_45
        )

        # Calculate IV/RV ratios
        rv30 = yang_zhang(df_price_history_3mo)
        iv30_rv30 = term_spline(30) / rv30
        iv30_rv30_call = call_term_spline(30) / rv30 if call_term_spline else iv30_rv30
        iv30_rv30_put = put_term_spline(30) / rv30 if put_term_spline else iv30_rv30

        rolling_share_volume = df_price_history_3mo["volume"].rolling(30).mean().dropna()
        rolling_dollar_volume = df_price_history_3mo["dollar_volume"].rolling(30).mean().dropna()

        if rolling_share_volume.empty:
            avg_share_volume = 0
        else:
            avg_share_volume = rolling_share_volume.iloc[-1]

        if rolling_dollar_volume.empty:
            avg_dollar_volume = 0
        else:
            avg_dollar_volume = rolling_dollar_volume.iloc[-1]

        expected_move_straddle = (straddle / underlying_price) if straddle else None
        expected_move_call = (call_mid / underlying_price) if call_mid else None
        expected_move_put = (put_mid / underlying_price) if put_mid else None

        (
            prev_earnings_avg_abs_pct_move,
            prev_earnings_median_abs_pct_move,
            prev_earnings_min_abs_pct_move,
            prev_earnings_max_abs_pct_move,
            prev_earnings_std,
            earnings_release_time,
            prev_earnings_values,
        ) = calc_prev_earnings_stats(self.bars_3y, ticker)

        if prev_earnings_values is None or not len(prev_earnings_values):
            prev_earnings_values = []

        result_summary = {
            "avg_30d_dollar_volume": round(avg_dollar_volume, 3),
            "avg_30d_dollar_volume_pass": avg_dollar_volume >= min_avg_30d_dollar_volume,
            "avg_30d_share_volume": round(avg_share_volume, 3),
            "avg_30d_share_volume_pass": avg_share_volume >= min_avg_30d_share_volume,
            "iv30_rv30_overall": round(iv30_rv30, 3),
            "iv30_rv30_call": round(iv30_rv30_call, 3),
            "iv30_rv30_put": round(iv30_rv30_put, 3),
            "iv30_rv30_pass": iv30_rv30 >= min_iv30_rv30,
            "ts_slope_0_45_overall": round(ts_slope_0_45, 6),
            "ts_slope_0_45_call": round(ts_slope_0_45_call, 6),
            "ts_slope_0_45_put": round(ts_slope_0_45_put, 6),
            "ts_slope_0_45_pass": ts_slope_0_45 <= max_ts_slope_0_45,
            "underlying_price": round(underlying_price, 5),
            "straddle_spread_pct": str(round(straddle_spread_pct * 100, 3)) + "%",
            "spread_tier": (
                1
                if straddle_spread_pct <= TIER_1_MAX_SPREAD_PCT
                else (
                    2
                    if straddle_spread_pct <= TIER_2_MAX_SPREAD_PCT
                    else 3 if straddle_spread_pct <= TIER_3_MAX_SPREAD_PCT else 4
                )
            ),
            "call_spread": (call_bid, call_ask, f"strike: {straddle_call_strike}"),
            "put_spread": (put_bid, put_ask, f"strike: {straddle_put_strike}"),
            "spread_tiers": {
                "call": (
                    1
                    if (call_ask - call_bid) / underlying_price <= TIER_1_MAX_SPREAD_PCT
                    else (
                        2
                        if (call_ask - call_bid) / underlying_price <= TIER_2_MAX_SPREAD_PCT
                        else (3 if (call_ask - call_bid) / underlying_price <= TIER_3_MAX_SPREAD_PCT else 4)
                    )
                ),
                "put": (
                    1
                    if (put_ask - put_bid) / underlying_price <= TIER_1_MAX_SPREAD_PCT
                    else (
                        2
                        if (put_ask - put_bid) / underlying_price <= TIER_2_MAX_SPREAD_PCT
                        else (3 if (put_ask - put_bid) / underlying_price <= TIER_3_MAX_SPREAD_PCT else 4)
                    )
                ),
            },
            "expected_pct_move_straddle": (expected_move_straddle * 100).round(3).astype(str) + "%",
            "expected_pct_move_call": ((expected_move_call * 100).round(3).astype(str) + "%" if expected_move_call else "N/A"),
            "expected_pct_move_put": ((expected_move_put * 100).round(3).astype(str) + "%" if expected_move_put else "N/A"),
            "straddle_pct_move_ge_hist_pct_move_pass": expected_move_straddle >= prev_earnings_avg_abs_pct_move,
            "prev_earnings_stats": {
                "avg_abs_pct_move": str(round(prev_earnings_avg_abs_pct_move * 100, 3)) + "%",
                "median_abs_pct_move": str(round(prev_earnings_median_abs_pct_move * 100, 3)) + "%",
                "min_abs_pct_move": str(round(prev_earnings_min_abs_pct_move * 100, 3)) + "%",
                "max_abs_pct_move": str(round(prev_earnings_max_abs_pct_move * 100, 3)) + "%",
                "values": [str(round(i * 100, 3)) + "%" for i in prev_earnings_values],
            },
            "earnings_release_time": earnings_release_time,
        }

        _update_result_summary(
            result_summary,
            expected_move_straddle,
            prev_earnings_min_abs_pct_move,
            prev_earnings_avg_abs_pct_move,
            prev_earnings_std,
            iv30_rv30,
            ts_slope_0_45,
            avg_dollar_volume,
            avg_share_volume,
            iv30_rv30_call=iv30_rv30_call,
            iv30_rv30_put=iv30_rv30_put,
            ts_slope_0_45_call=ts_slope_0_45_call,
            ts_slope_0_45_put=ts_slope_0_45_put,
            expected_move_call=expected_move_call,
            expected_move_put=expected_move_put,
        )
        return result_summary


def calc_prev_earnings_stats(df_history, ticker):
    df_history = df_history.copy()
    df_history["date"] = df_history["date"].dt.date
    df_history = df_history.sort_values("date")

    n_tries = 3
    i = 0
    while i < n_tries:
        df_earnings_dates = yf.Ticker(ticker).earnings_dates
        if df_earnings_dates is not None and not df_earnings_dates.empty:
            break
        i += 1

    df_earnings_dates = df_earnings_dates.reset_index()
    df_earnings_dates.columns = (
        df_earnings_dates.columns.str.strip()  # remove leading/trailing spaces
        .str.lower()  # lowercase
        .str.replace(r"[^\w\s]", "", regex=True)  # drop special chars like (%)
        .str.replace(" ", "_")  # spaces → underscores
    )

    if df_earnings_dates is None:
        return 0, 0, 0, 0, 0, 0, None

    df_earnings_dates = df_earnings_dates[df_earnings_dates["event_type"] == "Earnings"].copy()
    df_earnings_dates["date"] = df_earnings_dates["earnings_date"].dt.date

    def classify_release(dt):
        hour = dt.hour
        if hour < 9:
            return "pre-market"
        elif hour >= 9:
            return "post-market"

    df_earnings_dates["release_timing"] = df_earnings_dates["earnings_date"].apply(classify_release)
    df_earnings = df_earnings_dates.merge(df_history, on="date", how="left", suffixes=("", "_earnings"))
    df_earnings["next_date"] = df_earnings["date"] + pd.Timedelta(days=1)
    df_next = df_history.rename(columns=lambda c: f"{c}_next" if c != "date" else "next_date")
    df_flat = df_earnings.merge(df_next, on="next_date", how="left")
    df_flat["prev_close"] = df_flat["close"].shift(1)
    df_flat["pre_market_move"] = (df_flat["open"] - df_flat["prev_close"]) / df_flat["prev_close"]
    df_flat["post_market_move"] = (df_flat["open_next"] - df_flat["close"]) / df_flat["close"]

    df_flat["earnings_move"] = df_flat.apply(
        lambda row: (
            row["pre_market_move"]
            if row["release_timing"] == "pre-market"
            else row["post_market_move"] if row["release_timing"] == "post-market" else None
        ),
        axis=1,
    )

    avg_abs_pct_move = round(abs(df_flat["earnings_move"]).mean(), 3)
    prev_earnings_std = round(abs(df_flat["earnings_move"]).std(ddof=1), 3)
    median_abs_pct_move = round(abs(df_flat["earnings_move"]).median(), 3)
    min_abs_pct_move = round(abs(df_flat["earnings_move"]).min(), 3)
    max_abs_pct_move = round(abs(df_flat["earnings_move"]).max(), 3)
    earnings_release_timing_mode = df_flat["release_timing"].mode()
    release_time = earnings_release_timing_mode.iloc[0] if not earnings_release_timing_mode.empty else "unknown"
    prev_earnings_values = df_flat["earnings_move"].dropna().values

    if prev_earnings_std < 0.001:
        prev_earnings_std = 0.001  # avoid division by 0 or overly tight thresholds

    return (
        avg_abs_pct_move,
        median_abs_pct_move,
        min_abs_pct_move,
        max_abs_pct_move,
        prev_earnings_std,
        release_time,
        prev_earnings_values,
    )


def filter_dates(dates):
    today = datetime.today().date()
    cutoff_date = today + timedelta(days=45)

    sorted_dates = sorted(datetime.strptime(date, "%Y-%m-%d").date() for date in dates)

    arr = []
    for i, date in enumerate(sorted_dates):
        if date >= cutoff_date:
            arr = [d.strftime("%Y-%m-%d") for d in sorted_dates[: i + 1]]
            break

    if len(arr) > 0:
        if arr[0] == today.strftime("%Y-%m-%d"):
            return arr[1:]
        return arr

    raise ValueError("No date 45 days or more in the future found.")


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


def build_term_structure(days, ivs):
    days = np.array(days)
    ivs = np.array(ivs)

    # Sort by days
    sort_idx = days.argsort()
    days = days[sort_idx]
    ivs = ivs[sort_idx]

    _, unique_idx = np.unique(days, return_index=True)
    days = days[sorted(unique_idx)]
    ivs = ivs[sorted(unique_idx)]

    if len(days) < 2:
        warnings.warn(f"Not enough unique days to interpolate for ticker {ticker}.")
        return

    spline = interp1d(days, ivs, kind="linear", fill_value="extrapolate")

    def term_spline(dte):
        if dte < days[0]:
            return ivs[0]
        elif dte > days[-1]:
            return ivs[-1]
        else:
            return float(spline(dte))

    return term_spline


def calc_kelly_bet(
    p_win: float = KELLY_WIN_RATE,
    odds_decimal: float = KELLY_ODDS_DECIMAL,
    current_bankroll: float = KELLY_BANKROLL,
    pct_kelly=KELLY_FRACTIONAL,
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


def get_all_usa_tickers(use_yf_db=True, earnings_date=datetime.today().strftime("%Y-%m-%d")):
    ### FMP ###

    try:
        fmp_apikey = os.getenv("FMP_API_KEY")
        fmp_url = f"https://financialmodelingprep.com/api/v3/earning_calendar?from={earnings_date}&to={earnings_date}&apikey={fmp_apikey}"  # noqa: E501
        fmp_response = requests.get(fmp_url)
        df_fmp = pd.DataFrame(fmp_response.json())
        df_fmp_usa = df_fmp[df_fmp["symbol"].str.fullmatch(r"[A-Z]{1,4}") & ~df_fmp["symbol"].str.contains(r"[.-]")]

        fmp_usa_symbols = sorted(df_fmp_usa["symbol"].unique().tolist())
    except Exception:
        logging.warning("No FMP API Key found. Only using NASDAQ")
        fmp_usa_symbols = []

    ### NASDAQ ###

    nasdaq_url = f"https://api.nasdaq.com/api/calendar/earnings?date={earnings_date}"
    nasdaq_headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    nasdaq_response = requests.get(nasdaq_url, headers=nasdaq_headers)
    nasdaq_calendar = nasdaq_response.json().get("data").get("rows")
    df_nasdaq = pd.DataFrame(nasdaq_calendar)
    df_nasdaq = df_nasdaq[df_nasdaq["symbol"].str.fullmatch(r"[A-Z]{1,4}") & ~df_nasdaq["symbol"].str.contains(r"[.-]")]

    nasdaq_tickers = sorted(df_nasdaq["symbol"].unique().tolist())

    all_usa_earnings_tickers_today = sorted(list(set(fmp_usa_symbols + nasdaq_tickers)))

    return all_usa_earnings_tickers_today


def _calculate_side_metrics(
    iv30_rv30_side,
    ts_slope_0_45_side,
    expected_move_side,
    prev_earnings_avg_abs_pct_move,
    volume_return,
    ivrv_deciles,
    ts_deciles,
):
    """Calculate expected return for a specific option side (call or put)"""
    # IV to RV ratio return
    ivrv_return = 1.0
    for low, high, expected_return in ivrv_deciles:
        if low <= iv30_rv30_side < high:
            ivrv_return = expected_return
            break

    # Term structure return
    ts_return = 1.0
    for low, high, expected_return in ts_deciles:
        if low <= ts_slope_0_45_side < high:
            ts_return = expected_return
            break

    # Calculate combined expected return
    ivrv_profit = ivrv_return - 1.0
    ts_profit = ts_return - 1.0
    volume_profit = volume_return - 1.0
    combined_return = round(1.0 + ivrv_profit + ts_profit + volume_profit, 4)

    # Bonus return if expected move >= avg historical earnings move
    bonus_return = 0
    if expected_move_side >= prev_earnings_avg_abs_pct_move:
        bonus_return = min(0.075, (expected_move_side - prev_earnings_avg_abs_pct_move) / prev_earnings_avg_abs_pct_move)

    return round(combined_return + bonus_return, 4)


def _calculate_kelly_bet(improved_suggestion, original_suggestion, final_expected_return):
    """Calculate kelly bet based on suggestion type and expected return"""
    # If expected return is 0 or we're avoiding, both base and adjusted should be 0
    if final_expected_return <= 1.0 or "Tier 8" in improved_suggestion or "Avoid" in improved_suggestion:
        return 0, 0

    expected_profit_rate = final_expected_return - 1.0
    base_bet = round(expected_profit_rate * KELLY_BANKROLL * KELLY_FRACTIONAL, 3)

    if "Tier 1" in improved_suggestion:
        adjusted_bet = round(base_bet * 1.18, 2)
    elif "Tier 2" in improved_suggestion:
        adjusted_bet = round(base_bet * 1.15, 2)
    elif "Tier 3" in improved_suggestion:
        adjusted_bet = round(base_bet * 1.12, 2)
    elif "Tier 4" in improved_suggestion:
        adjusted_bet = round(base_bet * 1.09, 2)
    elif "Tier 5" in improved_suggestion:
        adjusted_bet = round(base_bet * 1.06, 2)
    elif "Tier 6" in improved_suggestion:
        adjusted_bet = round(base_bet * 1.03, 2)
    elif "Tier 7" in improved_suggestion:
        adjusted_bet = round(base_bet * 1.01, 2)
    elif "Consider" in improved_suggestion:
        adjusted_bet = round(base_bet / 2, 2)
    elif original_suggestion == "Consider":
        adjusted_bet = round(base_bet / 5, 2)
    else:
        adjusted_bet = round(base_bet, 2)

    return base_bet, min(adjusted_bet, MAX_KELLY_BET)


def _update_result_summary(
    result_summary,
    expected_move_straddle,
    prev_earnings_min_abs_pct_move,
    prev_earnings_avg_abs_pct_move,
    prev_earnings_std,
    iv30_rv30,
    ts_slope_0_45,
    avg_dollar_volume,
    avg_share_volume,
    iv30_rv30_call=None,
    iv30_rv30_put=None,
    ts_slope_0_45_call=None,
    ts_slope_0_45_put=None,
    expected_move_call=None,
    expected_move_put=None,
):
    """Original recommendation based on estimated probabilities (done in-place)"""
    if (
        result_summary["avg_30d_dollar_volume_pass"]
        and result_summary["iv30_rv30_pass"]
        and result_summary["ts_slope_0_45_pass"]
        and result_summary["avg_30d_share_volume_pass"]
    ):
        original_suggestion = "Recommended"
    elif result_summary["ts_slope_0_45_pass"] and (
        (result_summary["avg_30d_dollar_volume_pass"] and not result_summary["iv30_rv30_pass"])
        or (result_summary["iv30_rv30_pass"] and not result_summary["avg_30d_dollar_volume_pass"])
    ):
        original_suggestion = "Consider"
    else:
        original_suggestion = "Avoid"

    if (
        result_summary["avg_30d_dollar_volume"] >= TIER_1_AVG_30D_DOLLAR_VOLUME
        and result_summary["iv30_rv30_overall"] >= TIER_1_IV30_RV30
        and ts_slope_0_45 <= TIER_1_TS_SLOPE_0_45
        and result_summary["avg_30d_share_volume_pass"]
        and result_summary["underlying_price"] >= MIN_SHARE_PRICE_TIER_1
        and result_summary["spread_tier"] == 1
        and result_summary["straddle_pct_move_ge_hist_pct_move_pass"]
        and expected_move_straddle > prev_earnings_min_abs_pct_move  # safety filter - data quality check
    ):
        improved_suggestion = "Best Case Recommended - Tier 1"
    elif (
        result_summary["avg_30d_dollar_volume"] >= TIER_2_AVG_30D_DOLLAR_VOLUME
        and result_summary["iv30_rv30_overall"] >= TIER_2_IV30_RV30
        and ts_slope_0_45 <= TIER_2_TS_SLOPE_0_45
        and result_summary["avg_30d_share_volume_pass"]
        and result_summary["underlying_price"] >= MIN_SHARE_PRICE_TIER_2
        and result_summary["spread_tier"] <= 2
        and result_summary["straddle_pct_move_ge_hist_pct_move_pass"]
        and expected_move_straddle > prev_earnings_min_abs_pct_move  # safety filter - data quality check
    ):
        improved_suggestion = "Highly Recommended - Tier 2"
    elif (
        result_summary["avg_30d_dollar_volume"] >= TIER_3_AVG_30D_DOLLAR_VOLUME
        and result_summary["iv30_rv30_overall"] >= TIER_3_IV30_RV30
        and ts_slope_0_45 <= TIER_3_TS_SLOPE_0_45
        and result_summary["avg_30d_share_volume_pass"]
        and result_summary["underlying_price"] >= MIN_SHARE_PRICE_TIER_3
        and result_summary["spread_tier"] <= 3
        and result_summary["straddle_pct_move_ge_hist_pct_move_pass"]
        and expected_move_straddle > prev_earnings_min_abs_pct_move  # safety filter - data quality check
    ):
        improved_suggestion = "Highly Recommended - Tier 3"
    elif (
        result_summary["avg_30d_dollar_volume_pass"]
        and result_summary["iv30_rv30_pass"]
        and result_summary["ts_slope_0_45_pass"]
        and result_summary["avg_30d_share_volume_pass"]
        and result_summary["underlying_price"] >= MIN_SHARE_PRICE
        and result_summary["straddle_pct_move_ge_hist_pct_move_pass"]
        and expected_move_straddle > prev_earnings_min_abs_pct_move  # safety filter - data quality check
    ):
        improved_suggestion = "Recommended - Tier 4"
    elif (
        result_summary["avg_30d_dollar_volume_pass"]
        and result_summary["iv30_rv30_pass"]
        and result_summary["ts_slope_0_45_pass"]
        and result_summary["avg_30d_share_volume_pass"]
        and result_summary["underlying_price"] >= MIN_SHARE_PRICE
        and prev_earnings_avg_abs_pct_move - expected_move_straddle
        <= 0.75 * prev_earnings_std  # Avg move - Straddle is within 0.75 std deviations
        and expected_move_straddle > prev_earnings_min_abs_pct_move  # Safety filter - data quality check
    ):
        improved_suggestion = "Slightly Recommended - Tier 5"
    elif (
        result_summary["avg_30d_dollar_volume_pass"]
        and result_summary["iv30_rv30_pass"]
        and result_summary["ts_slope_0_45_pass"]
        and result_summary["avg_30d_share_volume_pass"]
        and result_summary["underlying_price"] >= MIN_SHARE_PRICE
        and prev_earnings_avg_abs_pct_move - expected_move_straddle
        <= 0.50 * prev_earnings_std  # Avg move - Straddle is within 0.50 std deviations
        and expected_move_straddle > prev_earnings_min_abs_pct_move  # Safety filter - data quality check
    ):
        improved_suggestion = "Recommended - Tier 6"
    elif (
        result_summary["ts_slope_0_45_pass"]
        and result_summary["avg_30d_dollar_volume_pass"]
        and result_summary["iv30_rv30_pass"]
        and expected_move_straddle * 1.5 < prev_earnings_min_abs_pct_move
    ):
        improved_suggestion = "Consider..."
    elif (
        result_summary["ts_slope_0_45_pass"]
        and result_summary["avg_30d_dollar_volume_pass"]
        and result_summary["iv30_rv30_pass"]
        and result_summary["underlying_price"] >= MIN_SHARE_PRICE / 2
    ):
        improved_suggestion = "Slightly Consider..."
    elif result_summary["ts_slope_0_45_pass"] and (
        (result_summary["avg_30d_dollar_volume_pass"] and not result_summary["iv30_rv30_pass"])
        or (result_summary["iv30_rv30_pass"] and not result_summary["avg_30d_dollar_volume_pass"])
    ):
        improved_suggestion = "Eh... Consider, but it's risky! - Tier 7"
    else:
        improved_suggestion = "Avoid - Tier 8"

    # IV to RV ratio
    ivrv_deciles = [
        (1.2543, 1.3358, 1.002),  # ~0.2% edge
        (1.3358, 1.4320, 1.003),  # ~0.3% edge
        (1.4320, 1.5602, 1.020),  # ~2.0% edge
        (1.5602, 1.7772, 1.033),  # ~3.3% edge
        (1.7772, 45.5241, 1.038),  # ~3.8% edge
    ]

    # Term structure slope deciles with edge multipliers
    ts_deciles = [
        (-0.6165, -0.0125, 1.085),  # 8.5% more edge
        (-0.0125, -0.0075, 1.060),  # 6% more edge
        (-0.0075, -0.0051, 1.038),  # 3.8% more edge
        (-0.0051, -0.00406, 1.085),  # 1.8% more edge
        # (-0.0051, -0.0036, 1.085),  # 1.8% more edge
    ]

    # Share volume deciles with edge multipliers -- original youtube analysis
    share_volume_deciles = [
        (1878818, 2536831, 1.003),  # 1.003% more edge
        (2536831, 3749598, 1.013),  # 1.30% more edge
        (3749598, 6743977, 1.026),  # 3.8% more edge
        (6743977, 737093575, 1.032),  # 3.2% more edge
    ]

    # Attempt to map the share volume to dollar volume buckets
    dollar_volume_deciles = [
        (5000000, 50000000, 1.005),
        (50000000, 200000000, 1.015),
        (200000000, 1000000000, 1.028),
        (1000000000, 100000000000, 1.038),
    ]

    ivrv_return = 1.0
    for low, high, expected_return in ivrv_deciles:
        if low <= iv30_rv30 < high:
            ivrv_return = expected_return
            break

    ts_return = 1.0
    for low, high, expected_return in ts_deciles:
        if low <= ts_slope_0_45 < high:
            ts_return = expected_return
            break

    dollar_volume_return = 1.0
    for low, high, expected_return in dollar_volume_deciles:
        if low <= avg_dollar_volume < high:
            dollar_volume_return = expected_return
            break

    share_volume_return = 1.0
    for low, high, expected_return in share_volume_deciles:
        if low <= avg_share_volume < high:
            share_volume_return = expected_return
            break

    volume_return = max(share_volume_return, dollar_volume_return)

    # Calculate combined expected return additively since factors are independent
    # Convert multipliers to profit percentages, add them, convert back to multiplier
    ivrv_profit = ivrv_return - 1.0  # e.g., 1.0175 → 0.0175 (1.75% profit)
    ts_profit = ts_return - 1.0  # e.g., 1.0235 → 0.0235 (2.35% profit)
    volume_profit = volume_return - 1.0  # e.g., 1.0285 → 0.0285 (2.85% profit)

    combined_expected_return = round(1.0 + ivrv_profit + ts_profit + volume_profit, 4)

    # Bonus return if straddle expected move >= avg historical earnings move (last 3 years)
    bonus_return = 0
    if expected_move_straddle >= prev_earnings_avg_abs_pct_move:
        bonus_return = min(0.075, (expected_move_straddle - prev_earnings_avg_abs_pct_move) / prev_earnings_avg_abs_pct_move)

    final_expected_return = round(combined_expected_return + bonus_return, 4)

    # Calculate call and put side metrics if available
    call_final_expected_return = final_expected_return
    put_final_expected_return = final_expected_return

    if all([iv30_rv30_call, ts_slope_0_45_call, expected_move_call]):
        call_final_expected_return = _calculate_side_metrics(
            iv30_rv30_call,
            ts_slope_0_45_call,
            expected_move_call,
            prev_earnings_avg_abs_pct_move,
            volume_return,
            ivrv_deciles,
            ts_deciles,
        )

    if all([iv30_rv30_put, ts_slope_0_45_put, expected_move_put]):
        put_final_expected_return = _calculate_side_metrics(
            iv30_rv30_put,
            ts_slope_0_45_put,
            expected_move_put,
            prev_earnings_avg_abs_pct_move,
            volume_return,
            ivrv_deciles,
            ts_deciles,
        )

    # If avoiding the trade, set expected returns to 0
    if "Tier 8" in improved_suggestion or "Avoid" in improved_suggestion:
        final_expected_return = 0
        call_final_expected_return = 0
        put_final_expected_return = 0

    # Calculate Kelly bets for overall and each side
    base_kelly_bet, adjusted_kelly_bet = _calculate_kelly_bet(improved_suggestion, original_suggestion, final_expected_return)
    base_kelly_bet_call, adjusted_kelly_bet_call = _calculate_kelly_bet(
        improved_suggestion, original_suggestion, call_final_expected_return
    )
    base_kelly_bet_put, adjusted_kelly_bet_put = _calculate_kelly_bet(
        improved_suggestion, original_suggestion, put_final_expected_return
    )

    # Store all results
    result_summary["improved_suggestion"] = improved_suggestion
    result_summary["improved_suggestion_call"] = improved_suggestion
    result_summary["improved_suggestion_put"] = improved_suggestion
    result_summary["original_suggestion"] = original_suggestion
    result_summary["original_suggestion_call"] = original_suggestion
    result_summary["original_suggestion_put"] = original_suggestion

    result_summary["final_expected_return"] = final_expected_return
    result_summary["final_expected_return_call"] = call_final_expected_return
    result_summary["final_expected_return_put"] = put_final_expected_return
    result_summary["base_kelly_bet"] = base_kelly_bet
    result_summary["base_kelly_bet_call"] = base_kelly_bet_call
    result_summary["base_kelly_bet_put"] = base_kelly_bet_put
    result_summary["adjusted_kelly_bet"] = adjusted_kelly_bet
    result_summary["adjusted_kelly_bet_call"] = adjusted_kelly_bet_call
    result_summary["adjusted_kelly_bet_put"] = adjusted_kelly_bet_put


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run calculations for given tickers")

    parser.add_argument(
        "--earnings-date",
        type=str,
        default=datetime.today().strftime("%Y-%m-%d"),
        help="Earnings date in YYYY-MM-DD format (default: today)",
    )

    parser.add_argument("--tickers", nargs="+", required=True, help="List of ticker symbols (e.g., NVDA AAPL TSLA)")

    parser.add_argument(
        "--verbose",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Verbose output for displaying all results. Default is True.",
    )

    args = parser.parse_args()
    earnings_date = args.earnings_date
    tickers = args.tickers
    verbose = args.verbose

    if tickers == ["_all"]:
        tickers = get_all_usa_tickers(earnings_date=earnings_date)

    logging.info(f"Scanning {len(tickers)} tickers: \n{tickers}\n")
    logging.info(f"Connecting to Interactive Brokers at {IB_HOST}:{IB_PORT} with client ID {IB_CLIENT_ID}")

    ib_client = ib.IB()

    try:
        ib_client.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, readonly=True)
        ib_client.reqMarketDataType(4)
        # Set longer timeout for IV calculations that may take time with deep ITM options
        ib_client.setTimeout(30)

        # mde = MarketDataExtractor(ticker="DIS")
        # mde.get_historical_bars()
        # mde.get_option_data()
        for ticker in tickers:
            iv_scanner = EarningsIVScanner(ticker=ticker)
            result = iv_scanner.compute_recommendation(ticker)
            is_edge = isinstance(result, dict) and "Recommended" in result.get("improved_suggestion")
            if is_edge:
                logging.info("*** EDGE FOUND ***\n")

            if verbose or is_edge:
                logging.info(f"ticker: {ticker}")
                if isinstance(result, dict):
                    for k, v in result.items():
                        logging.info(f"  {k}: {v}")
                else:
                    logging.info(f"  {result}")
                logging.info("---------------")

    except Exception as e:
        import traceback

        logging.error(f"Error connecting to Interactive Brokers: {e}")
        logging.error("Full traceback:")
        traceback.print_exc()
        logging.error("Make sure TWS or IB Gateway is running and accepting API connections")
    finally:
        if ib_client.isConnected():
            ib_client.disconnect()
            logging.info("Disconnected from Interactive Brokers")
