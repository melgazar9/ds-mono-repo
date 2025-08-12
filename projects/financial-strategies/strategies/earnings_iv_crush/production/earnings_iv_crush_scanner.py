import ib_async as ib
from datetime import datetime, timedelta
from scipy.interpolate import interp1d
import numpy as np
import argparse
import warnings
import pandas as pd
import os
import requests
import plotly.express as px
from dateutil.relativedelta import relativedelta
import yfinance as yf
import yaml

import logging

warnings.filterwarnings("ignore", message="Not enough unique days to interpolate for ticker")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# Load configuration
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


def get_option_historical_data(ib_client, contract):
    """
    Get option pricing data deterministically based on configuration
    - DELAYED data (dev or production): Use historical BID_ASK bars
    - REAL_TIME data in production: FAIL HARD if no live data (no historical fallback)
    """
    # For REAL_TIME in production: NO historical fallback allowed
    if MARKET_DATA_TYPE == "REAL_TIME" and not DEVELOPMENT_MODE:
        raise ValueError("Real-time production mode does not use historical data fallback")

    # For DELAYED data or REAL_TIME in development: Use historical BID_ASK data
    try:
        bars = ib_client.reqHistoricalData(
            contract=contract,
            endDateTime="",  # Most recent
            durationStr="1 D",
            barSizeSetting="30 mins",
            whatToShow="BID_ASK",
            useRTH=True,  # Regular trading hours only
            formatDate=1,
            keepUpToDate=False,
        )

        if bars and len(bars) > 0:
            last_bar = bars[-1]  # Most recent bar
            if DEVELOPMENT_MODE:
                logging.info(
                    f"Historical BID_ASK data: Date={last_bar.date}, Bid={getattr(last_bar, 'low', 'N/A')},"
                    f"Ask={getattr(last_bar, 'high', 'N/A')}, Mid={getattr(last_bar, 'close', 'N/A')}"
                )

            # For BID_ASK: high=ask, low=bid, close=mid
            return {
                "bid_price": getattr(last_bar, "low", None),
                "ask_price": getattr(last_bar, "high", None),
                "mid_price": getattr(last_bar, "close", None),
                "source": "historical_bid_ask_30mins",
            }
        else:
            logging.error("No historical bars returned")
            return None

    except Exception as e:
        logging.error(f"Failed to get historical data: {str(e)[:100]}")
        return None


def get_option_data_with_iv(ticker_obj, strike, ib_client=None, contract=None):
    """
    Extract option pricing data and implied volatility from IB ticker object
    If bid/ask are -1 (after hours), try historical data then previous data.

    Returns dict with bid, ask, lastPrice, impliedVolatility or None if no valid data
    """
    # Debug output in development mode - show ALL available fields
    if DEVELOPMENT_MODE:
        print(f"Debug ticker data for strike {strike}:")
        print(f"All ticker attributes: {[attr for attr in dir(ticker_obj) if not attr.startswith('_')]}")
        for attr in ["bid", "ask", "last", "close", "prevClose", "lastSize", "bidSize", "askSize", "volume", "halted"]:
            print(f"{attr}: {getattr(ticker_obj, attr, 'N/A')}")
        if hasattr(ticker_obj, "marketDataType"):
            print(f"marketDataType: {ticker_obj.marketDataType}")

    # Check if we have valid bid/ask prices (handle nan values properly)
    has_prices = (
        hasattr(ticker_obj, "bid")
        and ticker_obj.bid is not None
        and not np.isnan(ticker_obj.bid)
        and ticker_obj.bid > 0
        and hasattr(ticker_obj, "ask")
        and ticker_obj.ask is not None
        and not np.isnan(ticker_obj.ask)
        and ticker_obj.ask > 0
    )

    if not has_prices:
        raise ValueError(f"Could not get valid option prices for strike {strike}! Production safety check failed.")

    if not has_prices and DEVELOPMENT_MODE and ib_client and contract:
        invalid_msg = (
            f"Live data invalid (bid={getattr(ticker_obj, 'bid', 'N/A')}, "
            f"ask={getattr(ticker_obj, 'ask', 'N/A')}), trying historical data..."
        )

        print(invalid_msg)

        hist_data = get_option_historical_data(ib_client, contract)

        if hist_data:
            if "bid_price" in hist_data and "ask_price" in hist_data and hist_data["bid_price"] and hist_data["ask_price"]:
                # We have actual bid/ask data
                bid_price = hist_data["bid_price"]
                ask_price = hist_data["ask_price"]
                print(f"Using {hist_data['source']}: bid={bid_price:.2f}, ask={ask_price:.2f}")
            elif "close_price" in hist_data and hist_data["close_price"]:
                # DEVELOPMENT ONLY: Use closing price with estimated spread
                # WARNING: This estimation is unsafe for production trading
                close_price = hist_data["close_price"]
                spread_est = close_price * 0.015  # 1.5% spread estimate
                bid_price = close_price - spread_est / 2
                ask_price = close_price + spread_est / 2
                print(
                    f"DEV MODE: Using {hist_data['source']}: {close_price}, estimated bid/ask:"
                    f"{bid_price:.2f}/{ask_price:.2f}"
                )
                logging.warning("This is an estimate and would fail in production!")
            else:
                print("  Historical data found but no usable prices")
                return None
        else:
            # Fall back to previous data from ticker
            print("  No historical data, checking previous prices...")
            prev_bid = getattr(ticker_obj, "prevBid", None)
            prev_ask = getattr(ticker_obj, "prevAsk", None)
            prev_last = getattr(ticker_obj, "prevLast", None)

            print(f"Previous data: prevBid={prev_bid}, prevAsk={prev_ask}, prevLast={prev_last}")

            if prev_bid and prev_ask and not np.isnan(prev_bid) and not np.isnan(prev_ask) and prev_bid > 0 and prev_ask > 0:
                bid_price = prev_bid
                ask_price = prev_ask
                print(f"Using previous bid/ask: {bid_price:.2f}/{ask_price:.2f}")
            elif prev_last and not np.isnan(prev_last) and prev_last > 0:
                # DEVELOPMENT ONLY: Estimate spread from last price
                # WARNING: This estimation is unsafe for production trading
                spread_estimate = prev_last * 0.02
                bid_price = prev_last - spread_estimate / 2
                ask_price = prev_last + spread_estimate / 2
                print(f"DEV MODE: Using previous last: {prev_last}, estimated bid/ask: {bid_price:.2f}/{ask_price:.2f}")
                logging.warning("This is an estimate and would fail in production!")
            else:
                print("  No valid price data available")
                return None

    elif not has_prices:
        return None
    else:
        # Use live prices
        bid_price = ticker_obj.bid
        ask_price = ticker_obj.ask

    # Calculate midpoint IV from bid/ask Greeks (most accurate)
    bid_iv = None
    ask_iv = None

    # Debug Greeks data in development mode
    if DEVELOPMENT_MODE:
        print("Greeks debug:")
        print(f"bidGreeks: {getattr(ticker_obj, 'bidGreeks', 'N/A')}")
        print(f"askGreeks: {getattr(ticker_obj, 'askGreeks', 'N/A')}")
        print(f"modelGreeks: {getattr(ticker_obj, 'modelGreeks', 'N/A')}")
        print(f"lastGreeks: {getattr(ticker_obj, 'lastGreeks', 'N/A')}")
        print(f"impliedVolatility: {getattr(ticker_obj, 'impliedVolatility', 'N/A')}")

    if hasattr(ticker_obj, "bidGreeks") and ticker_obj.bidGreeks and hasattr(ticker_obj.bidGreeks, "impliedVol"):
        bid_iv = ticker_obj.bidGreeks.impliedVol
    if hasattr(ticker_obj, "askGreeks") and ticker_obj.askGreeks and hasattr(ticker_obj.askGreeks, "impliedVol"):
        ask_iv = ticker_obj.askGreeks.impliedVol

    # Use midpoint if both available, otherwise fall back to available one
    if bid_iv is not None and ask_iv is not None:
        iv = (bid_iv + ask_iv) / 2
    elif bid_iv is not None:
        iv = bid_iv
    elif ask_iv is not None:
        iv = ask_iv
    elif hasattr(ticker_obj, "modelGreeks") and ticker_obj.modelGreeks and hasattr(ticker_obj.modelGreeks, "impliedVol"):
        iv = ticker_obj.modelGreeks.impliedVol
    elif hasattr(ticker_obj, "lastGreeks") and ticker_obj.lastGreeks and hasattr(ticker_obj.lastGreeks, "impliedVol"):
        iv = ticker_obj.lastGreeks.impliedVol
    elif (
        hasattr(ticker_obj, "impliedVolatility") and ticker_obj.impliedVolatility and not np.isnan(ticker_obj.impliedVolatility)
    ):
        iv = ticker_obj.impliedVolatility
        if DEVELOPMENT_MODE:
            print(f"Using direct impliedVolatility: {iv}")
    else:
        # No IV available from any IB source (common during after-hours)
        if DEVELOPMENT_MODE:
            print("  No implied volatility available from IB (normal for after-hours)")
        iv = None

    # Production safety: IV is critical for options trading
    if not DEVELOPMENT_MODE and (iv is None or np.isnan(iv)):
        raise ValueError(f"Could not get implied volatility for strike {strike}! Production safety check failed.")

    return {
        "strike": strike,
        "bid": bid_price,
        "ask": ask_price,
        "lastPrice": ticker_obj.last if hasattr(ticker_obj, "last") and not np.isnan(ticker_obj.last) else None,
        "impliedVolatility": iv if iv and not np.isnan(iv) else None,
    }


def get_ib_historical_data(ib_client, ticker, duration="1 Y", bar_size="1 day"):
    """Get historical data from Interactive Brokers"""
    try:
        contract = ib.Stock(ticker, "SMART", "USD")
        ib_client.qualifyContracts(contract)

        bars = ib_client.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1,
        )

        if not bars:
            return None

        df = ib.util.df(bars)
        df.index = pd.to_datetime(df["date"])
        df["dollar_volume"] = df["volume"] * df["close"]

        return df

    except Exception as e:
        print(f"Error getting historical data for {ticker}: {e}")
        return None


def get_ib_options_chains_batch(ib_client, ticker):
    """Get options chains from IB using batch processing (faster)"""
    try:
        contract = ib.Stock(ticker, "SMART", "USD")
        ib_client.qualifyContracts(contract)

        # Get current stock price using historical data (faster than live)
        current_price = None
        try:
            recent_bars = ib_client.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr="1 D",
                barSizeSetting="1 min",
                whatToShow="TRADES",
                useRTH=False,
                formatDate=1,
            )
            if recent_bars:
                current_price = recent_bars[-1].close
                logging.info(f"Current price for {ticker}: ${current_price:.2f}")
        except Exception as e:
            print(f"Price data failed for {ticker}: {e}")
            return None, None

        if not current_price:
            return None, "Unable to get current price"

        # Get option chains
        chains = ib_client.reqSecDefOptParams(contract.symbol, "", contract.secType, contract.conId)
        if not chains:
            return None, None

        options_data = {}

        for chain in chains:
            if chain.exchange == "SMART":
                # Filter to only relevant expirations (within 75 days)
                today = datetime.today().date()
                max_date = today + timedelta(days=75)

                valid_expirations = []
                for expiration in sorted(chain.expirations):
                    exp_date_obj = datetime.strptime(expiration, "%Y%m%d").date()
                    if today <= exp_date_obj <= max_date:
                        valid_expirations.append(expiration)

                print(f"Processing {len(valid_expirations)} expirations within 75 days for {ticker}")

                # For each expiration, get only ATM strikes (much faster)
                for expiration in valid_expirations:
                    exp_date = datetime.strptime(expiration, "%Y%m%d").strftime("%Y-%m-%d")

                    try:
                        # Get contract details to find available strikes
                        call_contract_base = ib.Option(ticker, expiration, 0, "C", "SMART")
                        call_details = ib_client.reqContractDetails(call_contract_base)
                        available_strikes = sorted([detail.contract.strike for detail in call_details])

                        # if not available_strikes:
                        #     continue

                        # Find ATM strike (closest to current price)
                        atm_strike = min(available_strikes, key=lambda x: abs(x - current_price))
                        print(f"{exp_date}: Using ATM strike ${atm_strike} (current price: ${current_price:.2f})")

                        # Get only ATM call and put data
                        call_contract = ib.Option(ticker, expiration, atm_strike, "C", "SMART")
                        put_contract = ib.Option(ticker, expiration, atm_strike, "P", "SMART")

                        # Qualify contracts
                        ib_client.qualifyContracts(call_contract, put_contract)

                        # Request options market data with retry logic
                        max_retries = 2
                        call_data = None
                        put_data = None

                        for attempt in range(max_retries):
                            logging.info(
                                f"Requesting market data for {call_contract.localSymbol} and {put_contract.localSymbol}"
                                f"(attempt {attempt + 1}/{max_retries})"
                            )

                            # Try reqTickers instead of reqMktData
                            tickers = ib_client.reqTickers(call_contract, put_contract)
                            if len(tickers) >= 2:
                                call_ticker = tickers[0]
                                put_ticker = tickers[1]
                            else:
                                logging.error(f"Failed to get tickers, got {len(tickers)} instead of 2")
                                continue

                            call_data = get_option_data_with_iv(call_ticker, atm_strike, ib_client, call_contract)
                            put_data = get_option_data_with_iv(put_ticker, atm_strike, ib_client, put_contract)

                            if call_data and put_data:
                                logging.info(f"Successfully got option data on attempt {attempt + 1}")
                                break
                            else:
                                logging.warning(
                                    f"No valid data on attempt {attempt + 1},"
                                    f"{'retrying...' if attempt < max_retries - 1 else 'giving up'}"
                                )
                                # Cancel failed requests before retrying
                                try:
                                    ib_client.cancelMktData(call_contract)
                                    ib_client.cancelMktData(put_contract)
                                    # ib_client.cancelMktData(underlying_contract)
                                    ib_client.sleep(0.5)  # Brief pause before retry
                                except Exception:
                                    pass

                        if call_data and put_data:
                            options_data[exp_date] = {
                                "calls": pd.DataFrame([call_data]),
                                "puts": pd.DataFrame([put_data]),
                                "atm_strike": atm_strike,
                            }
                            print(
                                f"ATM Call IV: {call_data['impliedVolatility']:.3f}"
                                if call_data["impliedVolatility"] is not None
                                else "ATM Call IV: N/A"
                            )
                            print(
                                f"ATM Put IV: {put_data['impliedVolatility']:.3f}"
                                if put_data["impliedVolatility"] is not None
                                else "ATM Put IV: N/A"
                            )
                        else:
                            print(f"No valid option data for {exp_date}")

                        # try:
                        #     ib_client.cancelMktData(contract)
                        #     time.sleep(0.1)
                        #     ib_client.cancelMktData(call_contract)
                        #     time.sleep(0.1)
                        #     ib_client.cancelMktData(put_contract)
                        #     time.sleep(0.2)  # Give IB time to process cancellations
                        # except Exception as cleanup_error:
                        #     print(f"Cleanup error: {cleanup_error}")
                        #     pass

                    except Exception as e:
                        print(f"Error processing {exp_date}: {e}")
                        continue

                break

        return options_data, contract

    except Exception as e:
        print(f"Error getting batch options chains for {ticker}: {e}")
        return None, None


def get_ib_options_chains(ib_client, ticker):
    """Get options chains from Interactive Brokers"""
    try:
        contract = ib.Stock(ticker, "SMART", "USD")
        ib_client.qualifyContracts(contract)

        # Get current stock price using historical data (reliable fallback)
        current_price = None

        try:
            # Try recent 1-minute bar first (most current)
            recent_bars = ib_client.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr="1 D",
                barSizeSetting="1 min",
                whatToShow="TRADES",
                useRTH=False,  # Include extended hours for more recent data
                formatDate=1,
            )

            if recent_bars:
                current_price = recent_bars[-1].close  # Most recent close
                print(f"Using recent price for {ticker}: ${current_price:.2f}")
            else:
                # Fallback to daily bar
                daily_bars = ib_client.reqHistoricalData(
                    contract,
                    endDateTime="",
                    durationStr="2 D",
                    barSizeSetting="1 day",
                    whatToShow="TRADES",
                    useRTH=True,
                    formatDate=1,
                )
                if daily_bars:
                    current_price = daily_bars[-1].close
                    print(f"Using daily close for {ticker}: ${current_price:.2f}")
                else:
                    print(f"No price data available for {ticker}")

        except Exception as e:
            print(f"Price data unavailable for {ticker}: {e}")

        # Final check - if still no price, return error
        if not current_price or np.isnan(current_price):
            return None, f"Error: Unable to retrieve live price data for {ticker}. Check IB connection and data permissions."

        logging.info(f"Current price for {ticker}: ${current_price:.2f}")

        # Get option chains
        chains = ib_client.reqSecDefOptParams(contract.symbol, "", contract.secType, contract.conId)

        if not chains:
            return None, None

        options_data = {}

        for chain in chains:
            if chain.exchange == "SMART":
                all_expirations = sorted(chain.expirations)

                # Filter expirations to only include dates within 75 days
                today = datetime.today().date()
                max_date = today + timedelta(days=75)

                valid_expirations = []
                for expiration in all_expirations:
                    exp_date_obj = datetime.strptime(expiration, "%Y%m%d").date()
                    if today <= exp_date_obj <= max_date:
                        valid_expirations.append(expiration)

                print(f"Found {len(all_expirations)} total expirations, using {len(valid_expirations)} within 75 days")
                print(f"Getting all available option contracts for {ticker} within 75 days...")

                all_option_data = {}

                for expiration in valid_expirations:
                    exp_date = datetime.strptime(expiration, "%Y%m%d").strftime("%Y-%m-%d")
                    days_out = (datetime.strptime(expiration, "%Y%m%d").date() - today).days
                    print(f"Processing expiration {exp_date} ({days_out} days out)")

                    call_contract_base = ib.Option(ticker, expiration, 0, right="C", exchange="SMART", currency="USD")
                    put_contract_base = ib.Option(ticker, expiration, 0, "P", "SMART")

                    try:
                        call_details = ib_client.reqContractDetails(call_contract_base)
                        call_strikes = [detail.contract.strike for detail in call_details]
                    except Exception:
                        call_strikes = []

                    try:
                        put_details = ib_client.reqContractDetails(put_contract_base)
                        put_strikes = [detail.contract.strike for detail in put_details]
                    except Exception:
                        put_strikes = []

                    # Only keep strikes that exist for both calls and puts
                    common_strikes = sorted(set(call_strikes) & set(put_strikes))
                    all_option_data[exp_date] = {"expiration": expiration, "available_strikes": common_strikes}

                    print(f"Expiration {exp_date}: {len(common_strikes)} available strikes")

                # Now filter to only the strikes we actually want to price (+/- 50% of current price)
                min_strike = current_price * 0.5  # 50% below
                max_strike = current_price * 1.5  # 50% above

                print(f"Filtering to strikes between ${min_strike:.2f} and ${max_strike:.2f} (±50% of current price)")

                # Now iterate only over filtered strikes to get market data
                for exp_date, exp_data in all_option_data.items():
                    expiration = exp_data["expiration"]
                    available_strikes = exp_data["available_strikes"]

                    # Filter to our target range
                    target_strikes = [s for s in available_strikes if min_strike <= s <= max_strike]
                    target_strikes = sorted(target_strikes)

                    if not target_strikes:
                        print(f"No strikes in target range for {exp_date}, skipping")
                        continue

                    strikes_str = [f"${s}" for s in target_strikes]
                    print(f"Getting option market data for {len(target_strikes)} strikes in {exp_date}: {strikes_str}")

                    calls_list = []
                    puts_list = []

                    for strike in target_strikes:
                        call_contract = ib.Option(ticker, expiration, strike, "C", "SMART")
                        put_contract = ib.Option(ticker, expiration, strike, "P", "SMART")

                        try:
                            # Try to qualify contracts first - skip if they don't exist
                            try:
                                ib_client.qualifyContracts(call_contract, put_contract)
                            except Exception as contract_error:
                                if "security definition" in str(contract_error).lower():
                                    continue  # Skip this strike, contract doesn't exist
                                raise

                            # Get live options market data
                            call_data = None
                            put_data = None

                            try:
                                # Request snapshot data for options with underlying active for IV calculation
                                print(f"Requesting snapshot for {ticker} {strike} call/put...")

                                # Set market data type based on config
                                if MARKET_DATA_TYPE == "DELAYED":
                                    ib_client.reqMarketDataType(4)
                                else:
                                    ib_client.reqMarketDataType(1)

                                # Request option snapshots
                                call_ticker = ib_client.reqMktData(call_contract, "", True, False)  # snapshot=True
                                put_ticker = ib_client.reqMktData(put_contract, "", True, False)  # snapshot=True
                                # Wait for snapshots
                                ib_client.sleep(1.0)

                                # Extract option data using helper function (DRY)
                                call_data = get_option_data_with_iv(call_ticker, strike, ib_client, call_contract)
                                put_data = get_option_data_with_iv(put_ticker, strike, ib_client, put_contract)

                                # Print results
                                if call_data:
                                    call_iv_str = (
                                        f"{call_data['impliedVolatility']:.3f}" if call_data["impliedVolatility"] else "N/A"
                                    )
                                    call_msg = (
                                        f"Call {strike}: bid=${call_data['bid']:.2f}, "
                                        f"ask=${call_data['ask']:.2f}, IV={call_iv_str}"
                                    )
                                    print(call_msg)
                                if put_data:
                                    put_iv_str = (
                                        f"{put_data['impliedVolatility']:.3f}" if put_data["impliedVolatility"] else "N/A"
                                    )
                                    put_msg = (
                                        f"Put {strike}: bid=${put_data['bid']:.2f}, "
                                        f"ask=${put_data['ask']:.2f}, IV={put_iv_str}"
                                    )
                                    print(put_msg)

                                # Cancel market data subscriptions to avoid accumulating subscriptions
                                try:
                                    ib_client.cancelMktData(contract)  # underlying
                                    ib_client.cancelMktData(call_contract)  # call
                                    ib_client.cancelMktData(put_contract)  # put
                                except Exception:
                                    pass

                            except Exception as e:
                                print(f"Real-time market data failed for {ticker} {strike}: {e}")
                                # Clean up all subscriptions on error
                                try:
                                    ib_client.cancelMktData(contract)
                                    ib_client.cancelMktData(call_contract)
                                    ib_client.cancelMktData(put_contract)
                                except Exception:
                                    pass

                            # Add valid data to lists
                            if call_data:
                                calls_list.append(call_data)
                            if put_data:
                                puts_list.append(put_data)

                            # No need to cancel snapshot requests

                        except Exception as e:
                            print(f"Error getting option data for {ticker} {expiration} {strike}: {e}")
                            continue

                    if calls_list and puts_list:
                        options_data[exp_date] = {"calls": pd.DataFrame(calls_list), "puts": pd.DataFrame(puts_list)}

                break

        return options_data, contract

    except Exception as e:
        print(f"Error getting options chains for {ticker}: {e}")
        return None, None


def calc_prev_earnings_stats(df_history, ticker, plot_loc=PLOT_LOC):
    """
    Calculate previous earnings statistics using Yahoo Finance earnings dates
    but with IB price data
    """

    df_history = df_history.copy()
    if "date" not in df_history.columns and df_history.index.name == "date":
        df_history = df_history.reset_index()
    elif df_history.index.dtype == "datetime64[ns]":
        df_history = df_history.reset_index()
        df_history = df_history.rename(columns={"index": "date"})

    df_history["date"] = pd.to_datetime(df_history["date"]).dt.date
    df_history = df_history.sort_values("date")

    # Use Yahoo Finance for earnings dates only
    ticker_obj = yf.Ticker(ticker)

    n_tries = 3
    i = 0
    while i < n_tries:
        df_earnings_dates = ticker_obj.earnings_dates
        if df_earnings_dates is not None and not df_earnings_dates.empty:
            break
        i += 1

    if df_earnings_dates is None:
        return 0, 0, 0, 0, 0, 0, None

    df_earnings_dates = df_earnings_dates.reset_index()
    df_earnings_dates = df_earnings_dates[df_earnings_dates["Event Type"] == "Earnings"].copy()
    df_earnings_dates["date"] = df_earnings_dates["Earnings Date"].dt.date

    def classify_release(dt):
        hour = dt.hour
        if hour < 9:
            return "pre-market"
        elif hour >= 9:
            return "post-market"

    df_earnings_dates["release_timing"] = df_earnings_dates["Earnings Date"].apply(classify_release)
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

    if plot_loc and df_flat.shape[0]:
        df_flat["text"] = (df_flat["earnings_move"] * 100).round(2).astype(str) + "%"
        p = px.bar(
            x=df_flat["date"],
            y=df_flat["earnings_move"].round(3),
            color=df_flat.index.astype(str),
            text=df_flat["text"],
            title="Earnings % Move",
        )
        p.update_traces(textangle=0)

        full_path = os.path.join(plot_loc, f"{ticker}_{df_flat['date'].iloc[0].strftime('%Y-%m-%d')}.html")
        os.makedirs(plot_loc, exist_ok=True)
        p.write_html(full_path)
        print(f"Saved plot for ticker {ticker} here: {full_path}")

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
        warnings.warn("Not enough unique days to interpolate for ticker.")
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


def get_current_price(df_price_history_3mo):
    return df_price_history_3mo["close"].iloc[-1]


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


def get_all_usa_tickers(earnings_date=datetime.today().strftime("%Y-%m-%d")):
    ### FMP ###

    try:
        fmp_apikey = os.getenv("FMP_API_KEY")
        fmp_url = (
            f"https://financialmodelingprep.com/api/v3/earning_calendar?"
            f"from={earnings_date}&to={earnings_date}&apikey={fmp_apikey}"
        )
        fmp_response = requests.get(fmp_url)
        df_fmp = pd.DataFrame(fmp_response.json())
        df_fmp_usa = df_fmp[df_fmp["symbol"].str.fullmatch(r"[A-Z]{1,4}") & ~df_fmp["symbol"].str.contains(r"[.-]")]

        fmp_usa_symbols = sorted(df_fmp_usa["symbol"].unique().tolist())
    except Exception:
        print("No FMP API Key found. Only using NASDAQ")
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


def compute_recommendation(
    ticker,
    ib_client,
    min_avg_30d_dollar_volume=MIN_AVG_30D_DOLLAR_VOLUME,
    min_avg_30d_share_volume=MIN_AVG_30D_SHARE_VOLUME,
    min_iv30_rv30=MIN_IV30_RV30,
    max_ts_slope_0_45=MAX_TS_SLOPE_0_45,
    plot_loc=PLOT_LOC,
):
    ticker = ticker.strip().upper()
    if not ticker:
        return "No stock symbol provided."

    # Get historical data from IB
    df_history = get_ib_historical_data(ib_client, ticker)
    if df_history is None or df_history.empty:
        return f"Error: Unable to retrieve historical data for {ticker}"

    # Get options chains from IB using batch processing (faster)
    options_chains, underlying_contract = get_ib_options_chains_batch(ib_client, ticker)
    if not options_chains:
        if isinstance(underlying_contract, str):  # Error message
            return underlying_contract
        return f"Error: No options found for stock symbol '{ticker}'."

    try:
        # Filter dates to get relevant expirations
        exp_dates = list(options_chains.keys())
        exp_dates = filter_dates(exp_dates)
    except Exception:
        return "Error: Not enough option data."

    # Get 3-month price history for volume calculations
    df_price_history_3mo = df_history[df_history.index >= (pd.Timestamp.now(df_history.index.tz) - relativedelta(months=3))]
    df_price_history_3mo = df_price_history_3mo.sort_index()

    try:
        underlying_price = get_current_price(df_price_history_3mo)
        if underlying_price is None:
            raise ValueError("No market price found.")
    except Exception:
        return "Error: Unable to retrieve underlying stock price."

    atm_iv = {}
    atm_call_iv = {}
    atm_put_iv = {}
    straddle = None
    straddle_spread_pct = 1.0
    call_mid = None
    put_mid = None
    call_bid = call_ask = put_bid = put_ask = None

    i = 0
    for exp_date in exp_dates:
        if exp_date not in options_chains:
            continue

        calls = options_chains[exp_date]["calls"]
        puts = options_chains[exp_date]["puts"]

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
                        warn_msg = (
                            f"For ticker {ticker} straddle is either 0 or None from available "
                            "bid/ask spread... using nearest term strikes."
                        )
                        warnings.warn(warn_msg)
                        straddle = calls.iloc[call_idx + 1]["lastPrice"] + puts.iloc[put_idx + 1]["lastPrice"]
                    if not straddle:
                        warn_msg = (
                            f"For ticker {ticker} straddle is either 0 or None from available "
                            "bid/ask spread... using lastPrice."
                        )
                        warnings.warn(warn_msg)
                        straddle = calls.iloc[call_idx]["lastPrice"] + puts.iloc[call_idx]["lastPrice"]
                except (IndexError, KeyError):
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
    ) = calc_prev_earnings_stats(df_history.reset_index(), ticker, plot_loc=plot_loc)

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
            else 2 if straddle_spread_pct <= TIER_2_MAX_SPREAD_PCT else 3 if straddle_spread_pct <= TIER_3_MAX_SPREAD_PCT else 4
        ),
        "call_spread": (call_bid, call_ask),
        "put_spread": (put_bid, put_ask),
        "expected_pct_move_straddle": (expected_move_straddle * 100).round(3).astype(str) + "%",
        "expected_pct_move_call": (expected_move_call * 100).round(3).astype(str) + "%" if expected_move_call else "N/A",
        "expected_pct_move_put": (expected_move_put * 100).round(3).astype(str) + "%" if expected_move_put else "N/A",
        "straddle_pct_move_ge_hist_pct_move_pass": expected_move_straddle >= prev_earnings_avg_abs_pct_move,
        "prev_earnings_avg_abs_pct_move": str(round(prev_earnings_avg_abs_pct_move * 100, 3)) + "%",
        "prev_earnings_median_abs_pct_move": str(round(prev_earnings_median_abs_pct_move * 100, 3)) + "%",
        "prev_earnings_min_abs_pct_move": str(round(prev_earnings_min_abs_pct_move * 100, 3)) + "%",
        "prev_earnings_max_abs_pct_move": str(round(prev_earnings_max_abs_pct_move * 100, 3)) + "%",
        "prev_earnings_values": [str(round(i * 100, 3)) + "%" for i in prev_earnings_values],
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run calculations for given tickers using Interactive Brokers data")

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

    parser.add_argument("--ib-host", type=str, default=IB_HOST, help=f"Interactive Brokers host (default: {IB_HOST})")

    parser.add_argument("--ib-port", type=int, default=IB_PORT, help=f"Interactive Brokers port (default: {IB_PORT})")

    parser.add_argument(
        "--ib-client-id", type=int, default=IB_CLIENT_ID, help=f"Interactive Brokers client ID (default: {IB_CLIENT_ID})"
    )

    args = parser.parse_args()
    earnings_date = args.earnings_date
    tickers = args.tickers
    verbose = args.verbose
    ib_host = args.ib_host
    ib_port = args.ib_port
    ib_client_id = args.ib_client_id

    if tickers == ["_all"]:
        tickers = get_all_usa_tickers(earnings_date=earnings_date)

    def main_sync():
        # Connect to Interactive Brokers
        print(f"Connecting to Interactive Brokers at {ib_host}:{ib_port} with client ID {ib_client_id}")
        ib_client = ib.IB()

        try:
            # Use synchronous connection to avoid event loop issues
            # Set readonly=True to avoid requesting orders/positions data
            ib_client.connect(ib_host, ib_port, clientId=ib_client_id, readonly=True)

            # Set market data type based on configuration
            if MARKET_DATA_TYPE == "DELAYED":
                print("Requesting DELAYED data (type 3)...")
                ib_client.reqMarketDataType(3)  # 3 = Delayed data
                ib_client.sleep(1.0)  # Give more time for the type to be set
                print("DELAYED data request sent (15-minute delay)")
            elif MARKET_DATA_TYPE == "REAL_TIME":
                print("Requesting REAL-TIME data (type 1)...")
                ib_client.reqMarketDataType(1)  # 1 = Live/real-time data
                ib_client.sleep(1.0)
                print("REAL-TIME data request sent")
            else:
                print("Using default market data type")

            print("Connected to Interactive Brokers successfully!")

            # Warm up market data subscriptions to avoid first-run failures
            print("Warming up market data subscriptions...")
            try:
                # Request a simple stock ticker to establish subscription
                warm_up_contract = ib.Stock("SPY", "SMART", "USD")
                ib_client.qualifyContracts(warm_up_contract)
                ib_client.sleep(2.0)  # Give time for subscription to establish
                ib_client.cancelMktData(warm_up_contract)
                print("Market data subscriptions warmed up")
            except Exception as e:
                print(f"Warning: Could not warm up market data subscriptions: {e}")

            print(f"Scanning {len(tickers)} tickers: \n{tickers}\n")

            for ticker in tickers:
                # Now using synchronous functions
                result = compute_recommendation(ticker, ib_client)
                is_edge = isinstance(result, dict) and "Recommended" in result.get("improved_suggestion")
                if is_edge:
                    print(" *** EDGE FOUND ***\n")

                if verbose or is_edge:
                    print(f"ticker: {ticker}")
                    if isinstance(result, dict):
                        for k, v in result.items():
                            print(f"{k}: {v}")
                    else:
                        print(f"{result}")
                    print("---------------")

        except Exception as e:
            import traceback

            print(f"Error connecting to Interactive Brokers: {e}")
            print("Full traceback:")
            traceback.print_exc()
            print("Make sure TWS or IB Gateway is running and accepting API connections")
        finally:
            if ib_client.isConnected():
                ib_client.disconnect()
                print("Disconnected from Interactive Brokers")

    # Run the main function
    main_sync()
