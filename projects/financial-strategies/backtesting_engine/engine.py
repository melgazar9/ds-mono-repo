from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class Signal:
    symbol: str
    direction: str  # 'long', 'short', 'hedge'
    confidence: float
    metadata: Dict[str, Any] = None
    instrument_type: str = "stock"  # 'stock', 'option', 'future'
    legs: List[Dict] = None  # For multi-leg strategies (spreads, hedges)


@dataclass
class Order:
    symbol: str
    quantity: float
    price: float
    order_type: str
    timestamp: Any
    greeks: Dict[str, float] = None


@dataclass
class Fill:
    order_id: str
    quantity: float
    price: float
    timestamp: Any
    fees: float = 0.0
    slippage: float = 0.0


@dataclass
class Position:
    symbol: str
    quantity: float
    avg_price: float
    unrealized_pnl: float = 0.0
    instrument_type: str = "stock"
    greeks: Dict[str, float] = None
    related_positions: List[str] = None  # For hedging


class BaseStrategy(ABC):
    @abstractmethod
    def generate_signals(self, market_data, context) -> List[Signal]:
        pass

    def position_sizing(self, signal, portfolio) -> float:
        pass

    def should_hedge(self, position, market_data) -> Optional[Signal]:
        """Override for delta hedging strategies"""
        return None

    def get_metadata(self) -> Dict[str, Any]:
        pass


class OrderManager:
    def create_order(self, signal: Signal, size: float, price: float) -> Order:
        pass

    def execute_order(self, order: Order, market_data) -> Fill:
        pass

    # New: HFT and Options support
    def execute_hedge_order(self, hedge_signal: Signal, target_delta: float) -> Fill:
        """Execute delta hedging orders"""
        pass

    def get_order_book_depth(self, symbol: str) -> Dict[str, Any]:
        """For HFT strategies - get Level 2 data"""
        pass


class PositionManager:
    def open_position(self, fill: Fill) -> Position:
        pass

    def close_position(self, position: Position, exit_fill: Fill) -> Dict[str, Any]:
        pass

    def mark_to_market(
        self, positions: List[Position], prices: Dict[str, float]
    ) -> Dict[str, Any]:
        pass

    def get_portfolio_delta(self, positions: List[Position]) -> float:
        """Calculate total portfolio delta"""
        pass

    def group_related_positions(
        self, positions: List[Position]
    ) -> Dict[str, List[Position]]:
        """Group spread/hedge positions together"""
        pass


class PerformanceAnalyzer:
    def calc_trade_stats(self, closed_positions: List[Dict]) -> Dict[str, Any]:
        pass

    def generate_attribution(
        self, positions: List[Dict], segments: List[str]
    ) -> Dict[str, Any]:
        pass


class BacktestEngine:
    def __init__(self):
        self.order_manager = OrderManager()
        self.position_manager = PositionManager()
        self.performance_analyzer = PerformanceAnalyzer()

    def run_simulation(self, strategy: BaseStrategy, data_iterator) -> Dict[str, Any]:
        pass


# Example
class DeltaHedgedOptionsStrategy(BaseStrategy):
    def __init__(self, delta_threshold=0.15):
        self.delta_threshold = delta_threshold

    def generate_signals(self, market_data, context):
        return [Signal(symbol="AAPL", direction="short", instrument_type="option")]

    def should_hedge(self, position, market_data):
        if position.instrument_type == "option" and position.greeks:
            if abs(position.greeks["delta"]) > self.delta_threshold:
                return Signal(
                    symbol=position.symbol.replace("_OPT", ""),  # Underlying
                    direction="long" if position.greeks["delta"] < 0 else "short",
                    confidence=1.0,
                    metadata={"hedge_type": "delta", "target_delta": 0},
                )
        return None
