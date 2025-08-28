"""
Earnings IV Crush Scanner using Official IBKR API
Clean, DRY, PEP-8 compliant version with configurable timeouts
"""

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from datetime import datetime, timedelta
import argparse
import logging
import os
import threading
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def load_config():
    """Load configuration from YAML file"""
    config_path = os.path.join(os.path.dirname(__file__), "config.yml")
    with open(config_path, "r") as file:
        return yaml.safe_load(file)


CONFIG = load_config()

# Configuration constants
IB_HOST = CONFIG["ib"]["host"]
IB_PORT = CONFIG["ib"]["port"]
IB_CLIENT_ID = CONFIG["ib"]["client_id"]
MARKET_DATA_TYPE = CONFIG["market_data"]["type"]


class OptionComputation:
    """Simple data class to store option computation results"""

    def __init__(self, delta, gamma, vega, theta, implied_vol, opt_price, und_price):
        self.delta = delta
        self.gamma = gamma
        self.vega = vega
        self.theta = theta
        self.implied_vol = implied_vol
        self.opt_price = opt_price
        self.und_price = und_price


class IBKRWrapper(EWrapper):
    """Wrapper class to handle IBKR API callbacks with snake_case methods"""

    def __init__(self):
        EWrapper.__init__(self)
        self.next_req_id = 1
        self.market_data = {}
        self.contract_details = {}
        self.option_params = {}
        self.iv_calculations = {}
        self.option_price_calculations = {}
        self.data_events = {}
        self.connection_ready = threading.Event()

    def get_next_req_id(self):
        """Get next request ID and increment counter"""
        req_id = self.next_req_id
        self.next_req_id += 1
        return req_id

    def _set_event(self, req_id):
        """Helper method to set event if it exists"""
        if req_id in self.data_events:
            self.data_events[req_id].set()

    def nextValidId(self, orderId: int):
        """Callback when connection is established - IBKR API callback"""
        self.next_req_id = orderId
        self.connection_ready.set()
        logging.info(f"API connection ready, next order ID: {orderId}")

    def error(self, reqId, errorCode, errorString):
        """Handle API errors - IBKR API callback"""
        if errorCode in [2104, 2106, 2158]:  # Info messages
            logging.info(f"Info {errorCode}, reqId {reqId}: {errorString}")
        elif errorCode in [2103, 2157, 2107]:  # Warnings (including HMDS inactive)
            logging.warning(f"Warning {errorCode}, reqId {reqId}: {errorString}")
        else:
            logging.error(f"Error {errorCode}, reqId {reqId}: {errorString}")

    def tickPrice(self, reqId, tickType, price, attrib):
        """Handle live price updates - IBKR API callback (must use camelCase)"""
        if reqId not in self.market_data:
            self.market_data[reqId] = {}

        if tickType == 4:  # Last price
            self.market_data[reqId]["last"] = price
            logging.debug(f"Got last price {price} for reqId {reqId}")
            self._set_event(reqId)
        elif tickType == 1:  # Bid
            self.market_data[reqId]["bid"] = price
        elif tickType == 2:  # Ask
            self.market_data[reqId]["ask"] = price

    def contractDetails(self, reqId, contractDetails):
        """Store contract details - IBKR API callback"""
        if reqId not in self.contract_details:
            self.contract_details[reqId] = []
        self.contract_details[reqId].append(contractDetails)

    def contractDetailsEnd(self, reqId):
        """Signal end of contract details - IBKR API callback"""
        logging.debug(f"Contract details complete for reqId {reqId}")
        self._set_event(reqId)

    def securityDefinitionOptionalParameter(
        self, reqId, exchange, underlyingConId, tradingClass, multiplier, expirations, strikes
    ):
        """Store option chain parameters - IBKR API callback"""
        self.option_params[reqId] = {
            "exchange": exchange,
            "underlyingConId": underlyingConId,
            "tradingClass": tradingClass,
            "multiplier": multiplier,
            "expirations": expirations,
            "strikes": strikes,
        }

    def securityDefinitionOptionalParameterEnd(self, reqId):
        """Signal end of option parameters - IBKR API callback"""
        logging.debug(f"Option parameters complete for reqId {reqId}")
        self._set_event(reqId)

    def calculateImpliedVolatilityComplete(self, reqId, impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice):
        """Store IV calculation result - IBKR API callback"""
        self.iv_calculations[reqId] = {
            "implied_vol": impliedVol,
            "delta": delta,
            "opt_price": optPrice,
            "pv_dividend": pvDividend,
            "gamma": gamma,
            "vega": vega,
            "theta": theta,
            "und_price": undPrice,
        }
        self._set_event(reqId)

    def calculateOptionPriceComplete(self, reqId, optPrice, delta, gamma, vega, theta, undPrice):
        """Store option price calculation result - IBKR API callback"""
        self.option_price_calculations[reqId] = {
            "opt_price": optPrice,
            "delta": delta,
            "gamma": gamma,
            "vega": vega,
            "theta": theta,
            "und_price": undPrice,
        }
        self._set_event(reqId)


class IBKRClient(EClient):
    """Client class to interact with IBKR API"""

    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
        self.wrapper = wrapper

    def wait_for_data(self, req_id, timeout=30):
        """Wait for data to be received with configurable timeout"""
        if req_id not in self.wrapper.data_events:
            self.wrapper.data_events[req_id] = threading.Event()

        if self.wrapper.data_events[req_id].wait(timeout):
            return True
        else:
            logging.warning(f"Timeout waiting for data for reqId {req_id}")
            return False

    def wait_for_connection(self, timeout=30):
        """Wait for connection to be ready (nextValidId callback)"""
        if self.wrapper.connection_ready.wait(timeout):
            logging.info("Connection established successfully")
            return True
        else:
            logging.error(f"Connection timeout after {timeout}s")
            return False


class MarketDataExtractor:
    """Extract market data using official IBKR API with configurable timeouts"""

    def __init__(self, ticker: str, client: IBKRClient):
        self.ticker = ticker
        self.client = client
        self.wrapper = client.wrapper
        self.stock_contract = self._create_stock_contract(ticker)
        self.chain = None
        self.valid_expirations = None
        self.strikes = []

        # Try to get contract details, but don't fail if HMDS is down
        self._get_stock_contract_details()

    def _create_stock_contract(self, ticker: str) -> Contract:
        """Create a stock contract for the given ticker"""
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        return contract

    def _create_option_contract(self, ticker: str, expiration: str, strike: float, option_type: str) -> Contract:
        """Create an option contract"""
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "OPT"
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.lastTradeDateOrContractMonth = expiration
        contract.strike = strike
        contract.right = option_type
        contract.multiplier = "100"
        return contract

    def _wait_for_data_or_raise(self, req_id: int, error_msg: str, timeout: int = 30):
        """Wait for data or raise ValueError with custom message"""
        if not self.client.wait_for_data(req_id, timeout):
            raise ValueError(f"{error_msg} (timeout after {timeout}s)")

    def _get_stock_contract_details(self):
        """Get detailed contract information for the stock (optional)"""
        req_id = self.wrapper.get_next_req_id()
        self.client.reqContractDetails(req_id, self.stock_contract)

        if self.client.wait_for_data(req_id, 10):
            if req_id in self.wrapper.contract_details and self.wrapper.contract_details[req_id]:
                contract_details = self.wrapper.contract_details[req_id][0]
                self.stock_contract = contract_details.contract
                logging.info(f"Got contract details for {self.ticker}")
                return

        # If we can't get contract details, that's OK for basic stock contracts
        logging.warning(f"Could not get contract details for {self.ticker}, using basic contract")
        # Set a dummy conId to avoid errors later
        self.stock_contract.conId = 0

    def get_underlying_last_price(self):
        """Get current stock price via live market data"""
        req_id = self.wrapper.get_next_req_id()

        # Request live market data (no HMDS needed!)
        self.client.reqMktData(req_id, self.stock_contract, "", False, False, [])

        try:
            if self.client.wait_for_data(req_id, 10):
                if req_id in self.wrapper.market_data and "last" in self.wrapper.market_data[req_id]:
                    price = self.wrapper.market_data[req_id]["last"]
                    logging.info(f"Current price for {self.ticker}: ${price:.2f}")
                    return price

            raise ValueError(f"Could not get current price for {self.ticker}")

        finally:
            # Always cancel market data subscription
            self.client.cancelMktData(req_id)

    def get_option_chain(self, exchange="SMART"):
        """Get option chain parameters"""
        if not self.chain:
            req_id = self.wrapper.get_next_req_id()

            # Use conId if available, otherwise use 0 (will resolve automatically)
            con_id = getattr(self.stock_contract, "conId", 0)
            self.client.reqSecDefOptParams(req_id, self.ticker, "", "STK", con_id)

            self._wait_for_data_or_raise(req_id, f"Timeout getting option chain for {self.ticker}")

            if req_id in self.wrapper.option_params:
                self.chain = self.wrapper.option_params[req_id]
                logging.info(f"Got option chain for {self.ticker}")
            else:
                raise ValueError(f"No option chain found for {self.ticker}")

        return self

    def get_valid_expirations(self, max_days_out=75):
        """Filter expirations within specified days"""
        if not self.chain:
            self.get_option_chain()

        today = datetime.today().date()
        max_date = today + timedelta(days=max_days_out)
        self.valid_expirations = []

        for expiration in sorted(self.chain["expirations"]):
            exp_date_obj = datetime.strptime(expiration, "%Y%m%d").date()
            if today <= exp_date_obj <= max_date:
                self.valid_expirations.append(expiration)

        logging.info(f"Processing {len(self.valid_expirations)} expirations within {max_days_out} days for {self.ticker}")
        return self

    def get_option_qualified_contracts(self, strike_range=(0.95, 1.05)):
        """Get qualified option contracts for trading"""
        self.get_option_chain()
        current_price = self.get_underlying_last_price()

        # Filter strikes within range
        self.strikes = [
            strike
            for strike in self.chain["strikes"]
            if current_price * strike_range[0] < strike < current_price * strike_range[1]
        ]

        if not self.valid_expirations:
            self.get_valid_expirations()

        # Limit to first 3 expirations to avoid too many requests
        expirations = sorted(self.valid_expirations[:3])

        logging.info(f"Found {len(self.strikes)} strikes for {len(expirations)} expirations")
        return self

    def _get_option_price(self, option_contract: Contract, timeout: int = 15) -> float:
        """Get option price via live market data"""
        req_id = self.wrapper.get_next_req_id()

        # Try live market data first
        self.client.reqMktData(req_id, option_contract, "", False, False, [])

        try:
            if self.client.wait_for_data(req_id, timeout):
                if req_id in self.wrapper.market_data and "last" in self.wrapper.market_data[req_id]:
                    price = self.wrapper.market_data[req_id]["last"]
                    if price > 0:  # Valid price
                        return price

                # Try bid/ask if last price not available
                if req_id in self.wrapper.market_data:
                    data = self.wrapper.market_data[req_id]
                    if "bid" in data and "ask" in data and data["bid"] > 0 and data["ask"] > 0:
                        return (data["bid"] + data["ask"]) / 2.0

            raise ValueError("No valid option price available")

        finally:
            self.client.cancelMktData(req_id)

    def get_option_greeks(self, option_type: str, strike: float, exp: str, timeout: int = 15) -> OptionComputation:
        """
        Get option Greeks using market data with configurable timeout

        Args:
            option_type: "C" for call, "P" for put
            strike: Strike price
            exp: Expiration in YYYYMMDD format
            timeout: Timeout in seconds (default 15)

        Returns:
            OptionComputation object with Greeks and IV
        """
        current_price = self.get_underlying_last_price()

        # Create option contract
        option_contract = self._create_option_contract(self.ticker, exp, strike, option_type)

        # Get contract details first
        req_id = self.wrapper.get_next_req_id()
        self.client.reqContractDetails(req_id, option_contract)

        self._wait_for_data_or_raise(
            req_id, f"Timeout getting contract details for {self.ticker} {strike} {option_type}", timeout
        )

        if req_id not in self.wrapper.contract_details or not self.wrapper.contract_details[req_id]:
            raise ValueError(f"No contract found for {self.ticker} {strike} {option_type}")

        qualified_option = self.wrapper.contract_details[req_id][0].contract

        # Get option price from live market data
        option_price = self._get_option_price(qualified_option, timeout)

        # Calculate implied volatility with custom timeout
        iv_req_id = self.wrapper.get_next_req_id()
        self.client.calculateImpliedVolatility(iv_req_id, qualified_option, option_price, current_price, [])

        self._wait_for_data_or_raise(iv_req_id, f"IV calculation timed out for {self.ticker} {strike} {option_type}", timeout)

        if iv_req_id not in self.wrapper.iv_calculations:
            raise ValueError(f"No IV calculation result for {self.ticker} {strike} {option_type}")

        iv_result = self.wrapper.iv_calculations[iv_req_id]

        if iv_result["implied_vol"] <= 0 or iv_result["implied_vol"] is None:
            raise ValueError(f"Invalid IV for {self.ticker} {strike} {option_type}")

        # Calculate option price and Greeks using the IV
        greeks_req_id = self.wrapper.get_next_req_id()
        self.client.calculateOptionPrice(greeks_req_id, qualified_option, iv_result["implied_vol"], current_price, [])

        self._wait_for_data_or_raise(
            greeks_req_id, f"Greeks calculation timed out for {self.ticker} {strike} {option_type}", timeout
        )

        if greeks_req_id not in self.wrapper.option_price_calculations:
            raise ValueError(f"No Greeks calculation result for {self.ticker} {strike} {option_type}")

        greeks_result = self.wrapper.option_price_calculations[greeks_req_id]

        return OptionComputation(
            delta=greeks_result["delta"],
            gamma=greeks_result["gamma"],
            vega=greeks_result["vega"],
            theta=greeks_result["theta"],
            implied_vol=iv_result["implied_vol"],
            opt_price=greeks_result["opt_price"],
            und_price=greeks_result["und_price"],
        )

    def get_option_data(self):
        """Get option data for all strikes and expirations"""
        if not hasattr(self, "strikes") or not self.strikes:
            self.get_option_qualified_contracts()

        for exp in self.valid_expirations:
            for strike in self.strikes:
                put_greeks = None
                call_greeks = None

                try:
                    put_greeks = self.get_option_greeks("P", strike, exp)
                except ValueError as e:
                    logging.warning(f"No put options data for {self.ticker} strike {strike}: {e}")

                try:
                    call_greeks = self.get_option_greeks("C", strike, exp)
                except ValueError as e:
                    logging.warning(f"No call options data for {self.ticker} strike {strike}: {e}")

                if put_greeks or call_greeks:
                    put_info = (
                        f"Δ={put_greeks.delta:.3f}, Γ={put_greeks.gamma:.4f}, Θ={put_greeks.theta:.3f}, "
                        f"ν={put_greeks.vega:.3f}, IV={put_greeks.implied_vol:.3f}"
                        if put_greeks
                        else "N/A"
                    )
                    call_info = (
                        f"Δ={call_greeks.delta:.3f}, Γ={call_greeks.gamma:.4f}, Θ={call_greeks.theta:.3f}, "
                        f"ν={call_greeks.vega:.3f}, IV={call_greeks.implied_vol:.3f}"
                        if call_greeks
                        else "N/A"
                    )
                    logging.info(f"Strike {strike}: Put[{put_info}], Call[{call_info}]")

        return self


def main():
    """Main function to run the earnings IV crush scanner"""
    parser = argparse.ArgumentParser(description="Run earnings IV crush scanner with IBKR API")
    parser.add_argument("--tickers", nargs="+", required=True, help="List of ticker symbols")
    parser.add_argument("--timeout", type=int, default=15, help="Timeout for IV calculations (seconds)")

    args = parser.parse_args()

    logging.info(f"Scanning tickers: {args.tickers}")
    logging.info(f"Connecting to Interactive Brokers at {IB_HOST}:{IB_PORT}")

    # Create IBKR client and wrapper
    wrapper = IBKRWrapper()
    client = IBKRClient(wrapper)

    try:
        # Connect to IBKR
        client.connect(IB_HOST, IB_PORT, IB_CLIENT_ID)

        # Start client thread
        client_thread = threading.Thread(target=client.run, daemon=True)
        client_thread.start()

        # Wait for proper connection establishment (nextValidId callback)
        if not client.wait_for_connection(timeout=10):
            raise ConnectionError("Failed to establish IBKR connection")

        # Set market data type
        client.reqMarketDataType(MARKET_DATA_TYPE)

        # Process each ticker
        for ticker in args.tickers:
            try:
                logging.info(f"Processing {ticker}...")
                mde = MarketDataExtractor(ticker=ticker, client=client)
                mde.get_option_data()
            except Exception as e:
                logging.error(f"Error processing {ticker}: {e}")

    except Exception as e:
        logging.error(f"Error connecting to Interactive Brokers: {e}")
    finally:
        if client.isConnected():
            client.disconnect()
            logging.info("Disconnected from Interactive Brokers")


if __name__ == "__main__":
    main()
