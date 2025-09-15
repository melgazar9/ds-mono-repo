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
import time
from zoneinfo import ZoneInfo

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
        if self.bars_3y is None or not self.bars_3y.shape:
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
            # Find matching chain, with fallback handling
            matching_chain = None
            for c in chains:
                if c.tradingClass == self.ticker and c.exchange == exchange:
                    matching_chain = c
                    break

            if matching_chain is None:
                # Try with any exchange if SMART doesn't work
                for c in chains:
                    if c.tradingClass == self.ticker:
                        matching_chain = c
                        logging.warning(f"Using exchange {c.exchange} instead of {exchange} for {self.ticker}")
                        break

            if matching_chain is None:
                raise ValueError(f"No option chain found for ticker {self.ticker} on any exchange")

            self.chain = matching_chain
        return self

    def get_valid_expirations(self, max_days_out=75):
        today = datetime.now(ZoneInfo("America/Chicago")).date()
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
            logging.error(
                f"No expiration date 45 days or more in the future found for ticker {ticker}. "
                f"Calendar spread strategy not viable. Skipping..."
            )

        logging.info(f"Processing {len(self.valid_expirations)} expirations within 75 days for {self.ticker}")
        self.valid_expirations = sorted(self.valid_expirations)
        return self

    def _get_strikes_for_expiration(self, expiration, underlying_price, strike_range):
        """Get ALL available strikes for a specific expiration by testing them."""
        candidate_strikes = [
            s for s in self.chain.strikes if underlying_price * strike_range[0] < s < underlying_price * strike_range[1]
        ]

        test_contracts = [ib.Option(self.ticker, expiration, strike, "C", "SMART") for strike in candidate_strikes]

        try:
            qualified = ib_client.qualifyContracts(*test_contracts)
            return [c.strike for c in qualified if c is not None]
        except Exception as e:
            logging.error(f"Failed to test strikes for {self.ticker} expiration {expiration}: {e}")
            return []

    def get_option_qualified_contracts(self, strike_range=(0.85, 1.15)):
        """Get option contracts using actual intersection of available strikes across expirations."""
        self.get_option_chain()
        underlying_price = self.get_underlying_last_price()

        if not self.valid_expirations:
            self.get_valid_expirations()

        # Get ACTUAL available strikes for each expiration by testing them
        expiration_strikes = {}
        for exp in self.valid_expirations:
            strikes = self._get_strikes_for_expiration(exp, underlying_price, strike_range)
            expiration_strikes[exp] = strikes
            logging.info(f"Found {len(strikes)} available strikes for {self.ticker} expiration {exp}")

        # Find TRUE intersection of strikes available across ALL expirations
        if len(self.valid_expirations) >= 2:
            common_strikes = set(expiration_strikes[self.valid_expirations[0]])
            for exp in self.valid_expirations[1:]:
                common_strikes &= set(expiration_strikes[exp])
            self.strikes = sorted(common_strikes)
            logging.info(
                f"Intersection: {len(self.strikes)} common strikes across all "
                f"{len(self.valid_expirations)} expirations for {self.ticker}"
            )
        else:
            self.strikes = expiration_strikes.get(self.valid_expirations[0], [])

        # FAIL if no valid intersection - don't use stupid fallbacks
        if not self.strikes:
            logging.error(f"NO COMMON STRIKES found across expirations for {self.ticker} - calendar spread not possible")
            return []

        if DEBUG:
            logging.debug(
                f"DEBUG: Found {len(self.valid_expirations)} expirations and {len(self.strikes)} strikes for {self.ticker}"
            )

        # Create contracts ONLY for strikes we KNOW exist
        contracts = [
            ib.Option(self.ticker, exp, strike, right, "SMART")
            for right in ["P", "C"]
            for exp in self.valid_expirations
            for strike in self.strikes
        ]

        try:
            self.option_qualified_contracts = ib_client.qualifyContracts(*contracts)
            if not self.option_qualified_contracts:
                logging.error(f"Contract qualification completely failed for {self.ticker}")
                return []

            valid_count = len([c for c in self.option_qualified_contracts if c is not None])
            logging.info(
                f"Qualified {valid_count}/{len(contracts)} contracts for {self.ticker} (should be 100% since we pre-tested)"
            )

            if valid_count != len(contracts):
                logging.warning(f"Pre-testing failed: expected {len(contracts)} qualified contracts but got {valid_count}")

            return self
        except Exception as e:
            logging.error(f"Contract qualification failed for {self.ticker}: {e}")
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
            ib_client.sleep(0.1)
            iv_result = ib_client.calculateImpliedVolatility(qualified_option, option_price, current_price)
            if iv_result and hasattr(iv_result, "impliedVol") and iv_result.impliedVol > 0:
                iv = iv_result.impliedVol
            else:
                # TODO: Currently instead of a fallback to HV, in production this should error out
                # We can set it to use HV as a fallback if debug is set to True
                logging.warning(
                    f"IV calculation failed for {self.ticker} {strike} {option_type} - using historical volatility fallback"
                )
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

            # Filter out None contracts and maintain mapping
            valid_contracts = []
            valid_contract_map = {}
            for i, contract in enumerate(qualified_contracts):
                if contract is not None:
                    valid_contracts.append(contract)
                    # Use the original contract from all_contracts for the mapping
                    original_contract = all_contracts[i]
                    valid_contract_map[id(contract)] = contract_map[id(original_contract)]

            if len(valid_contracts) != len(qualified_contracts):
                logging.warning(f"Filtered out {len(qualified_contracts) - len(valid_contracts)} failed contracts")

            if not valid_contracts:
                logging.error(f"No valid qualified contracts for {self.ticker}")
                return []

            logging.info(f"Successfully qualified {len(valid_contracts)} out of {len(all_contracts)} contracts")
            # TODO: Is the below necessary?
            qualified_contracts = valid_contracts
            contract_map = valid_contract_map
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
        processed_count = 0  # Track progress

        start_time = time.time()

        # Cache HV calculation
        if not hasattr(self, "_hv30_cache"):
            if self.bars_3mo is None or self.bars_3mo.empty:
                self.get_historical_bars_3mo()
            self._hv30_cache = yang_zhang(self.bars_3mo)

        logging.info(f"Starting Greeks calculations for {len(qualified_contracts)} contracts for {self.ticker}")

        for contract, ticker in zip(qualified_contracts, all_tickers):
            processed_count += 1
            try:
                # Get contract details from our updated map
                contract_id = id(contract)
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

                # Calculate Greeks with optimized performance and error handling
                try:
                    # Only calculate IV for ATM and near-ATM options to save time
                    strike_diff_pct = abs(strike - current_price) / current_price
                    if strike_diff_pct > 0.10:  # Skip far OTM options (>10% away) - more aggressive
                        logging.debug(f"Skipping IV calculation for far OTM option {self.ticker} {strike} {option_type}")
                        market_data["greeks"] = None
                        market_data["iv"] = self._hv30_cache  # Use cached HV for far OTM
                    else:
                        # Minimal sleep time for production speed
                        ib_client.sleep(0.1)

                        # IV calculation with timeout handling
                        iv_result = None
                        try:
                            iv_result = ib_client.calculateImpliedVolatility(contract, option_price, current_price)
                        except Exception as timeout_error:
                            if "timeout" in str(timeout_error).lower():
                                logging.warning(
                                    f"IV calculation timeout for {self.ticker} {strike} {option_type} - using fallback"
                                )
                            else:
                                logging.warning(
                                    f"IV calculation error for {self.ticker} {strike} {option_type}: {timeout_error}"
                                )

                        if iv_result and hasattr(iv_result, "impliedVol") and iv_result.impliedVol > 0:
                            iv = iv_result.impliedVol
                        else:
                            # TODO: For near-ATM options we were using HV as a fallback. In production this should error out.
                            # We can set it to use HV as a fallback if debug is set to True
                            logging.warning(
                                f"IV calculation failed for {self.ticker} {strike} {option_type} - "
                                f"using historical volatility fallback"
                            )
                            iv = self._hv30_cache

                        greeks = ib_client.calculateOptionPrice(contract, iv, current_price)
                        market_data["greeks"] = greeks
                        market_data["iv"] = iv

                        # Progress logging every 10th contract with time estimate
                        if processed_count % 10 == 0:
                            elapsed_time = time.time() - start_time
                            rate = processed_count / elapsed_time if elapsed_time > 0 else 0
                            remaining = len(qualified_contracts) - processed_count
                            eta_seconds = remaining / rate if rate > 0 else 0
                            logging.info(
                                f"Progress: {processed_count}/{len(qualified_contracts)} contracts "
                                f"processed for {self.ticker} (ETA: {eta_seconds:.0f}s)"
                            )

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
        today = datetime.now(ZoneInfo("America/Chicago")).date()
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

        logging.error("No date 45 days or more in the future found. Calendar spread strategy not viable. Skipping...")
        return []

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
    ):
        ticker = ticker.strip().upper()
        if not ticker:
            return "No stock symbol provided."

        option_results = self.get_all_option_data()
        options_chains = self._parse_option_chain(option_results)

        if self.bars_3mo is None or self.bars_3mo.empty:
            self.get_historical_bars_3mo()

        df_price_history_3mo = self.bars_3mo.sort_values(by="date").copy()
        df_price_history_3mo["dollar_volume"] = df_price_history_3mo["volume"] * df_price_history_3mo["close"]

        try:
            underlying_price = self.get_underlying_last_price()
            if underlying_price is None:
                raise ValueError("No market price found.")
        except Exception as e:
            return f"Error: Unable to retrieve underlying stock price. {e}"

        atm_iv = {}
        atm_call_ivs = {}
        atm_put_ivs = {}
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
            call_atm_iv = calls.loc[call_idx, "impliedVolatility"]

            put_diffs = (puts["strike"] - underlying_price).abs()
            put_idx = put_diffs.idxmin()
            put_atm_iv = puts.loc[put_idx, "impliedVolatility"]

            # Validate IV calculations - warn but continue with fallbacks if needed
            if not call_atm_iv or call_atm_iv <= 0:
                logging.warning(f"Invalid call ATM IV {call_atm_iv} for {ticker} {exp_date}. Using fallback volatility.")
                call_atm_iv = 0.25  # 25% fallback IV
            if not put_atm_iv or put_atm_iv <= 0:
                logging.warning(f"Invalid put ATM IV {put_atm_iv} for {ticker} {exp_date}. Using fallback volatility.")
                put_atm_iv = 0.25  # 25% fallback IV

            atm_iv_value = (call_atm_iv + put_atm_iv) / 2.0
            atm_iv[exp_date] = atm_iv_value
            atm_call_ivs[exp_date] = call_atm_iv
            atm_put_ivs[exp_date] = put_atm_iv

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

                if call_mid is not None and put_mid is not None and call_mid > 0 and put_mid > 0:
                    straddle = call_mid + put_mid

                    # Validate spread calculations
                    if call_ask > call_bid and put_ask > put_bid:
                        call_spread = call_ask - call_bid
                        put_spread = put_ask - put_bid
                        total_straddle_spread = call_spread + put_spread

                        if underlying_price > 0:
                            straddle_spread_pct = total_straddle_spread / underlying_price
                        else:
                            logging.warning(f"Invalid underlying price {underlying_price} for spread calculation on {ticker}")
                            straddle_spread_pct = 1.0  # High spread
                    else:
                        logging.warning(f"Invalid bid/ask spread for {ticker} - using conservative spread estimate")
                        straddle_spread_pct = 1.0  # High spread to flag as avoid

                else:
                    straddle_spread_pct = 1.0  # Set high spread to fail all tier checks
                    try:
                        if call_idx + 1 < len(calls) and put_idx + 1 < len(puts):
                            # TODO: No fallbacks to near term strikes in production! Only in debug mode is ok!
                            warnings.warn(
                                f"For ticker {ticker} straddle is either 0 or None from "
                                f"available bid/ask spread... using nearest term strikes."
                            )
                            straddle = calls.iloc[call_idx + 1]["lastPrice"] + puts.iloc[put_idx + 1]["lastPrice"]
                        if not straddle:
                            # TODO: No fallbacks to near lastPrice in production! Only in debug mode is ok!
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

        today = datetime.now(ZoneInfo("America/Chicago")).date()
        dtes = []
        ivs = []
        for exp_date, iv in atm_iv.items():
            exp_date_obj = datetime.strptime(exp_date, "%Y-%m-%d").date()
            days_to_expiry = (exp_date_obj - today).days
            dtes.append(days_to_expiry)
            ivs.append(iv)

        # Build separate term structures for calls and puts
        call_dtes = []
        call_ivs = []
        put_dtes = []
        put_ivs = []
        for exp_date in atm_call_ivs.keys():
            exp_date_obj = datetime.strptime(exp_date, "%Y-%m-%d").date()
            days_to_expiry = (exp_date_obj - today).days
            call_dtes.append(days_to_expiry)
            call_ivs.append(atm_call_ivs[exp_date])
            put_dtes.append(days_to_expiry)
            put_ivs.append(atm_put_ivs[exp_date])

        overall_term_spline = build_term_structure(dtes, ivs)
        if not overall_term_spline:
            return

        call_term_spline = build_term_structure(call_dtes, call_ivs)
        put_term_spline = build_term_structure(put_dtes, put_ivs)

        # Calculate term structure slopes
        ts_slope_0_45 = (overall_term_spline(45) - overall_term_spline(dtes[0])) / (45 - dtes[0])
        ts_slope_0_45_call = (
            (call_term_spline(45) - call_term_spline(call_dtes[0])) / (45 - call_dtes[0]) if call_term_spline else ts_slope_0_45
        )
        ts_slope_0_45_put = (
            (put_term_spline(45) - put_term_spline(put_dtes[0])) / (45 - put_dtes[0]) if put_term_spline else ts_slope_0_45
        )

        # Calculate IV/RV ratios
        rv30 = yang_zhang(df_price_history_3mo)
        iv30_rv30 = overall_term_spline(30) / rv30
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

        # Calculate expected moves with robust validation
        expected_move_straddle = None
        expected_move_call = None
        expected_move_put = None

        # PRODUCTION VALIDATION: Check for critical data issues
        if not straddle or straddle <= 0:
            logging.warning(f"Cannot calculate straddle price for {ticker}. Strategy recommendation will be 'Avoid'.")

        if not underlying_price or underlying_price <= 0:
            raise ValueError(
                f"CRITICAL: Invalid underlying price {underlying_price} for {ticker}. Cannot proceed with invalid market data."
            )

        if straddle and straddle > 0 and underlying_price and underlying_price > 0:
            expected_move_straddle = straddle / underlying_price

        if call_mid and call_mid > 0 and underlying_price and underlying_price > 0:
            expected_move_call = call_mid / underlying_price

        if put_mid and put_mid > 0 and underlying_price and underlying_price > 0:
            expected_move_put = put_mid / underlying_price

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
            "expected_pct_move_straddle": (
                str(round(expected_move_straddle * 100, 3)) + "%" if expected_move_straddle else "N/A"
            ),
            "expected_pct_move_call": (str(round(expected_move_call * 100, 3)) + "%" if expected_move_call else "N/A"),
            "expected_pct_move_put": (str(round(expected_move_put * 100, 3)) + "%" if expected_move_put else "N/A"),
            "straddle_pct_move_ge_hist_pct_move_pass": (
                expected_move_straddle >= prev_earnings_avg_abs_pct_move if expected_move_straddle else False
            ),
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


# TODO: This function has a major bug! It is not working properly --> cross validate against yfinance.
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
    today = datetime.now(ZoneInfo("America/Chicago")).date()
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

    logging.error("No date 45 days or more in the future found. Calendar spread strategy not viable.")
    return []


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


def get_all_usa_tickers(use_yf_db=True, earnings_date=datetime.now(ZoneInfo("America/Chicago")).strftime("%Y-%m-%d")):
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
    if expected_move_straddle and expected_move_straddle >= prev_earnings_avg_abs_pct_move:
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


class OrderManager:
    """Manages automated order placement for earnings IV crush calendar spreads

    Trading Rules:
    - OPEN trades at exactly 14:45 CST (2:45 PM Central)
    - CLOSE trades at exactly 8:15 AM CST next day
    - Start with bid/ask pricing, increment $0.03 every 5 seconds until filled
    - Order size: 1 contract per leg to start
    - ALWAYS starts in PAPER TRADING mode for safety!
    """

    def __init__(self, ib_client, config, paper_trading=True):
        self.ib_client = ib_client
        self.config = config
        self.paper_trading = paper_trading
        self.active_positions = {}
        self.pending_orders = {}

        # Load order management settings from config
        order_config = config.get("order_management", {})
        self.order_size = order_config.get("order_size", 1)
        self.PRICE_INCREMENT = order_config.get("price_increment", 0.03)
        self.INCREMENT_INTERVAL = order_config.get("increment_interval", 5)
        self.max_increments = order_config.get("max_increments", 60)

        # Trading times (CST)
        open_time = order_config.get("open_time_cst", [14, 45])
        close_time = order_config.get("close_time_cst", [8, 15])
        self.OPEN_TIME_CST = tuple(open_time)
        self.CLOSE_TIME_CST = tuple(close_time)

        if not paper_trading:
            logging.warning("⚠️  LIVE TRADING MODE ENABLED - REAL MONEY AT RISK! ⚠️")
        else:
            logging.info("📝 PAPER TRADING MODE - Orders will be simulated only")

    def should_place_orders(self, analysis_result):
        """Determine if analysis meets criteria for automated trading"""
        if not isinstance(analysis_result, dict):
            return False

        # Check if ts_slope indicates edge (overvalued short-term IV)
        ts_slope = analysis_result.get("ts_slope_0_45_overall", 0)
        ts_threshold = self.config["screening"]["max_ts_slope_0_45"]  # -0.005

        # Check if it's a recommended trade (Tier 1-4)
        suggestion = analysis_result.get("improved_suggestion", "")
        is_recommended = "Recommended" in suggestion and "Tier" in suggestion

        # Check tier level (only auto-trade high confidence tiers)
        if is_recommended:
            tier_num = self._extract_tier_number(suggestion)
            if tier_num and tier_num <= 4:  # Only Tier 1-4 for automation
                return ts_slope <= ts_threshold

        return False

    def _extract_tier_number(self, suggestion_text):
        """Extract tier number from suggestion string"""
        import re

        match = re.search(r"Tier (\d+)", suggestion_text)
        return int(match.group(1)) if match else None

    def is_trading_time(self, target_time="open"):
        """Check if current time matches trading schedule (CST)"""
        from datetime import datetime
        import pytz

        # Get current CST time
        cst = pytz.timezone("America/Chicago")
        now_cst = datetime.now(cst)
        current_time = (now_cst.hour, now_cst.minute)

        if target_time == "open":
            return current_time == self.OPEN_TIME_CST
        elif target_time == "close":
            return current_time == self.CLOSE_TIME_CST

        return False

    def calculate_strikes(self, analysis_result):
        """Calculate strike prices using midpoint of implied and historical moves"""
        underlying_price = analysis_result.get("underlying_price", 0)

        # Get implied move from straddle
        implied_move_pct = analysis_result.get("expected_pct_move_straddle", 0) / 100

        # Get historical median move
        prev_stats = analysis_result.get("prev_earnings_stats", {})
        if "median_abs_pct_move" in prev_stats:
            hist_move_str = prev_stats["median_abs_pct_move"].replace("%", "")
            historical_move_pct = float(hist_move_str) / 100
        else:
            # Fallback to implied move if no historical data
            historical_move_pct = implied_move_pct

        # Take midpoint of implied and historical moves
        target_move_pct = (implied_move_pct + historical_move_pct) / 2

        # Calculate strikes (same for calls and puts - strangle style)
        call_strike = underlying_price * (1 + target_move_pct)
        put_strike = underlying_price * (1 - target_move_pct)

        # Round to nearest valid strike
        call_strike = self._round_to_valid_strike(call_strike)
        put_strike = self._round_to_valid_strike(put_strike)

        logging.info(
            f"Strike calculation: Implied {implied_move_pct:.1%}, "
            f"Historical {historical_move_pct:.1%}, Target {target_move_pct:.1%}"
        )
        logging.info(f"Calculated strikes: Call ${call_strike}, Put ${put_strike}")

        return call_strike, put_strike

    def _round_to_valid_strike(self, strike_price):
        """Round strike to valid increment"""
        if strike_price < 50:
            return round(strike_price * 2) / 2  # Round to nearest 0.5
        elif strike_price < 100:
            return round(strike_price)  # Round to nearest 1.0
        else:
            return round(strike_price / 2.5) * 2.5  # Round to nearest 2.5

    def get_market_prices(self, contracts):
        """Get current bid/ask prices for contracts"""
        try:
            if self.paper_trading:
                # Return simulated prices for paper trading
                return [(2.50, 2.60) for _ in contracts]  # (bid, ask)

            tickers = self.ib_client.reqTickers(*contracts)
            prices = []

            for ticker in tickers:
                if ticker.bid and ticker.ask and ticker.bid > 0 and ticker.ask > 0:
                    prices.append((ticker.bid, ticker.ask))
                else:
                    # Fallback prices if no market data
                    prices.append((1.00, 1.10))

            return prices

        except Exception as e:
            logging.error(f"Error getting market prices: {e}")
            # Return fallback prices
            return [(1.00, 1.10) for _ in contracts]

    def detect_calendar_spread_orders(self, ticker, analysis_result):
        """Create calendar spread orders with initial bid/ask pricing"""
        try:
            call_strike, put_strike = self.calculate_strikes(analysis_result)

            logging.info(f"Creating calendar spread for {ticker} (Order size: {self.order_size}):")
            logging.info(f"  Call strike: ${call_strike}, Put strike: ${put_strike}")

            # TODO: Get actual option expirations from chain data
            # self.strikes
            short_exp = "20240920"  # ~20 DTE
            long_exp = "20241018"  # ~50 DTE

            # Create option contracts
            short_call = ib.Option(ticker, short_exp, call_strike, "C", "SMART")
            long_call = ib.Option(ticker, long_exp, call_strike, "C", "SMART")
            short_put = ib.Option(ticker, short_exp, put_strike, "P", "SMART")
            long_put = ib.Option(ticker, long_exp, put_strike, "P", "SMART")

            contracts = [short_call, long_call, short_put, long_put]

            # Get current market prices
            prices = self.get_market_prices(contracts)

            # Create orders with bid/ask pricing
            orders = []

            # Short call (SELL) - start at ASK price (aggressive)
            short_call_price = prices[0][1]  # ask price
            short_call_order = ib.LimitOrder("SELL", self.order_size, short_call_price)
            orders.append((contracts[0], short_call_order, "SELL"))

            # Long call (BUY) - start at BID price (aggressive)
            long_call_price = prices[1][0]  # bid price
            long_call_order = ib.LimitOrder("BUY", self.order_size, long_call_price)
            orders.append((contracts[1], long_call_order, "BUY"))

            # Short put (SELL) - start at ASK price
            short_put_price = prices[2][1]  # ask price
            short_put_order = ib.LimitOrder("SELL", self.order_size, short_put_price)
            orders.append((contracts[2], short_put_order, "SELL"))

            # Long put (BUY) - start at BID price
            long_put_price = prices[3][0]  # bid price
            long_put_order = ib.LimitOrder("BUY", self.order_size, long_put_price)
            orders.append((contracts[3], long_put_order, "BUY"))

            logging.info("Initial pricing:")
            for contract, order, action in orders:
                logging.info(f"  {action} {contract.symbol} {contract.strike}{contract.right} @ ${order.lmtPrice:.2f}")

            return orders

        except Exception as e:
            logging.error(f"Error creating calendar spread orders for {ticker}: {e}")
            return None

    def place_orders_with_increments(self, orders, ticker):
        """Place orders and increment prices every 5 seconds until filled"""
        if self.paper_trading:
            logging.info(f"📝 PAPER TRADING: Simulating order placement for {ticker}")
            # Simulate immediate fills for paper trading
            for contract, order, action in orders:
                logging.info(
                    f"  📝 FILLED: {action} {order.totalQuantity} {contract.symbol} "
                    f"{contract.strike}{contract.right} @ ${order.lmtPrice:.2f}"
                )
            return True

        # LIVE TRADING - Place orders and monitor fills
        from datetime import datetime

        logging.info(f"⚠️  Placing LIVE orders for {ticker} at {datetime.now().strftime('%H:%M:%S')}")

        try:
            trades = []
            unfilled_orders = []

            # Place initial orders
            for contract, order, action in orders:
                trade = self.ib_client.placeOrder(contract, order)
                trades.append(trade)
                unfilled_orders.append((trade, contract, order, action))
                logging.info(
                    f"⚠️  ORDER PLACED: {action} {order.totalQuantity} "
                    f"{contract.symbol} {contract.strike}{contract.right} @ ${order.lmtPrice:.2f}"
                )

            # Monitor and increment prices until all filled
            increment_count = 0

            while unfilled_orders and increment_count < self.max_increments:
                self.ib_client.sleep(self.INCREMENT_INTERVAL)
                increment_count += 1

                # Check for fills
                still_unfilled = []
                for trade, contract, order, action in unfilled_orders:
                    if trade.isDone():
                        logging.info(f"✅ FILLED: {action} {contract.symbol} {contract.strike}{contract.right}")
                    else:
                        # Increment price and modify order
                        if action == "BUY":
                            # For buys, increase price (move toward ask)
                            new_price = order.lmtPrice + self.PRICE_INCREMENT
                        else:  # SELL
                            # For sells, decrease price (move toward bid)
                            new_price = order.lmtPrice - self.PRICE_INCREMENT

                        order.lmtPrice = max(0.01, round(new_price, 2))  # Don't go below $0.01
                        self.ib_client.placeOrder(contract, order)  # Modify existing order

                        logging.info(
                            f"🔄 PRICE INCREMENT #{increment_count}: {action} "
                            f"{contract.symbol} {contract.strike}{contract.right} "
                            f"@ ${order.lmtPrice:.2f}"
                        )
                        still_unfilled.append((trade, contract, order, action))

                unfilled_orders = still_unfilled

            # Check final results
            if unfilled_orders:
                logging.warning(
                    f"⚠️  {len(unfilled_orders)} orders for {ticker} did not fill after {increment_count} increments"
                )
                # Cancel remaining orders
                for trade, _, _, _ in unfilled_orders:
                    self.ib_client.cancelOrder(trade.order)
                return False
            else:
                logging.info(f"✅ All orders for {ticker} filled successfully!")
                self.active_positions[ticker] = {
                    "trades": trades,
                    "open_time": datetime.now(),
                    "strategy": "calendar_spread",
                    "status": "open",
                }
                return True

        except Exception as e:
            logging.error(f"⚠️  Error placing orders for {ticker}: {e}")
            return False

    def schedule_position_close(self, ticker):
        """Schedule position to close at 8:15 AM CST next day"""
        from datetime import datetime, timedelta
        import pytz

        # Calculate next day 8:15 AM CST
        cst = pytz.timezone("America/Chicago")
        tomorrow = datetime.now(cst) + timedelta(days=1)
        close_time = tomorrow.replace(hour=8, minute=15, second=0, microsecond=0)

        logging.info(f"📅 Scheduled to close {ticker} position at {close_time.strftime('%Y-%m-%d %H:%M:%S CST')}")

        # Store close time with position
        if ticker in self.active_positions:
            self.active_positions[ticker]["scheduled_close"] = close_time

    def close_position(self, ticker):
        """Close existing position using same bid/ask increment logic"""
        from datetime import datetime

        if ticker not in self.active_positions:
            logging.error(f"No active position found for {ticker}")
            return False

        position = self.active_positions[ticker]
        if position["status"] != "open":
            logging.warning(f"Position for {ticker} is not open (status: {position['status']})")
            return False

        logging.info(f"🔚 Closing position for {ticker} at {datetime.now().strftime('%H:%M:%S')}")

        # For paper trading, just simulate the close
        if self.paper_trading:
            logging.info(f"📝 PAPER TRADING: Simulating position close for {ticker}")
            position["status"] = "closed"
            position["close_time"] = datetime.now()
            return True

        # LIVE TRADING: Create closing orders (reverse the original positions)
        try:
            # Get the original trades from the position
            original_trades = position.get("trades", [])
            if not original_trades:
                logging.error(f"No original trades found for {ticker} position")
                return False

            closing_orders = []

            # Reverse each original trade
            for trade in original_trades:
                contract = trade.contract
                original_order = trade.order

                # Reverse the action: BUY becomes SELL, SELL becomes BUY
                reverse_action = "SELL" if original_order.action == "BUY" else "BUY"

                # Get current market data for pricing
                ticker_data = self.ib_client.reqTickers(contract)[0]

                # Set initial price based on action
                if reverse_action == "BUY":
                    # Buying to close short position - start at ASK
                    initial_price = ticker_data.ask if ticker_data.ask and ticker_data.ask > 0 else 1.00
                else:
                    # Selling to close long position - start at BID
                    initial_price = ticker_data.bid if ticker_data.bid and ticker_data.bid > 0 else 1.00

                # Create closing order
                closing_order = ib.LimitOrder(reverse_action, original_order.totalQuantity, initial_price)
                closing_orders.append((contract, closing_order, reverse_action))

                logging.info(
                    f"Creating closing order: {reverse_action} {closing_order.totalQuantity} "
                    f"{contract.symbol} {contract.strike}{contract.right} @ ${closing_order.lmtPrice:.2f}"
                )

            # Place closing orders with increment logic
            success = self._place_closing_orders_with_increments(closing_orders, ticker)

            if success:
                logging.info(f"✅ Position closed successfully for {ticker}")
                position["status"] = "closed"
                position["close_time"] = datetime.now()
                return True
            else:
                logging.error(f"❌ Failed to close position for {ticker}")
                return False

        except Exception as e:
            logging.error(f"Error closing position for {ticker}: {e}")
            return False

    def _place_closing_orders_with_increments(self, orders, ticker):
        """Place closing orders - DRY implementation using shared order logic"""
        if self.paper_trading:
            return self._simulate_paper_trading_orders(orders, ticker, "CLOSING")

        return self._execute_live_orders_with_increments(orders, ticker, "CLOSING")

    def process_trading_signal(self, ticker, analysis_result):
        """Main method to process trading signal at 14:45 CST"""
        # Check if it's trading time
        if not self.is_trading_time("open"):
            current_time = datetime.now().strftime("%H:%M CST")
            logging.debug(f"Not trading time for {ticker} (current: {current_time}, target: 14:45 CST)")
            return False

        if not self.should_place_orders(analysis_result):
            return False

        logging.info(f"🎯 TRADING SIGNAL AT 14:45 CST FOR {ticker} 🎯")

        # Detect contracts that should be traded
        orders = self.detect_calendar_spread_orders(ticker, analysis_result)
        # Create and place orders
        if not orders:
            logging.error(f"Failed to create orders for {ticker}")
            return False

        success = self.place_orders_with_increments(orders, ticker)
        if success:
            logging.info(f"✅ Successfully opened position for {ticker}")
            self.schedule_position_close(ticker)
        else:
            logging.error(f"❌ Failed to open position for {ticker}")

        return success

    def check_scheduled_closes(self):
        """Check for positions that need to be closed at 8:15 AM CST"""
        if not self.is_trading_time("close"):
            return

        from datetime import datetime

        for ticker, position in self.active_positions.items():
            if (
                position["status"] == "open"
                and "scheduled_close" in position
                and datetime.now() >= position["scheduled_close"].replace(tzinfo=None)
            ):

                logging.info(f"⏰ Time to close position for {ticker}")
                self.close_position(ticker)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run calculations for given tickers")

    parser.add_argument(
        "--earnings-date",
        type=str,
        default=datetime.now(ZoneInfo("America/Chicago")).strftime("%Y-%m-%d"),
        help="Earnings date in YYYY-MM-DD format (default: today)",
    )

    parser.add_argument("--tickers", nargs="+", required=True, help="List of ticker symbols (e.g., NVDA AAPL TSLA)")

    parser.add_argument(
        "--verbose",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Verbose output for displaying all results. Default is True.",
    )

    parser.add_argument(
        "--auto-trade",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable automated trading (PAPER TRADING MODE by default)",
    )

    parser.add_argument(
        "--live-trading",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable LIVE trading mode (⚠️  REAL MONEY AT RISK ⚠️ )",
    )

    args = parser.parse_args()
    earnings_date = args.earnings_date
    tickers = args.tickers
    verbose = args.verbose
    auto_trade = args.auto_trade
    live_trading = args.live_trading

    if tickers == ["_all"]:
        tickers = get_all_usa_tickers(earnings_date=earnings_date)

    if auto_trade and live_trading:
        logging.warning("⚠️  LIVE TRADING MODE REQUESTED ⚠️")
        confirmation = input(
            "⚠️  You are requesting LIVE TRADING mode with REAL MONEY. Type 'CONFIRM LIVE TRADING' to proceed: "
        )
        if confirmation != "CONFIRM LIVE TRADING":
            logging.info("Live trading not confirmed. Switching to paper trading mode.")
            live_trading = False
    elif auto_trade:
        logging.info("📝 Automated PAPER TRADING mode enabled")

    logging.info(f"Scanning {len(tickers)} tickers: \n{tickers}\n")
    logging.info(f"Connecting to Interactive Brokers at {IB_HOST}:{IB_PORT} with client ID {IB_CLIENT_ID}")

    ib_client = ib.IB()

    try:
        # Connect to IB (readonly=False if auto trading enabled)
        readonly_mode = not auto_trade
        ib_client.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, readonly=readonly_mode)
        ib_client.reqMarketDataType(4)
        ib_client.setTimeout(30)

        # Initialize OrderManager if auto-trading is enabled
        order_manager = None
        if auto_trade:
            paper_trading = not live_trading  # Paper trading unless explicitly live
            order_manager = OrderManager(ib_client, config, paper_trading=paper_trading)
            logging.info(f"🤖 OrderManager initialized ({'LIVE' if live_trading else 'PAPER'} trading mode)")

        for ticker in tickers:
            iv_scanner = EarningsIVScanner(ticker=ticker)
            result = iv_scanner.compute_recommendation(ticker)
            is_edge = isinstance(result, dict) and "Recommended" in result.get("improved_suggestion", "")

            if is_edge:
                logging.info("*** EDGE FOUND ***\n")

                # Process trading signal if auto-trading enabled
                if order_manager:
                    try:
                        order_manager.process_trading_signal(ticker, result)
                    except Exception as e:
                        logging.error(f"Error processing trading signal for {ticker}: {e}")

            if verbose or is_edge:
                logging.info(f"ticker: {ticker}")
                if isinstance(result, dict):
                    for k, v in result.items():
                        logging.info(f"  {k}: {v}")
                else:
                    logging.info(f"  {result}")
                logging.info("---------------")

        # Check for scheduled position closes if auto-trading enabled
        if order_manager:
            logging.info("🔍 Checking for scheduled position closes...")
            order_manager.check_scheduled_closes()

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
