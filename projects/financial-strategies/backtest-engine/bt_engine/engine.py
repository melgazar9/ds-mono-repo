from abc import abstractmethod
import pandas as pd
import polars as pl

from ds_core.ds_utils import MetaclassMethodEnforcer


"""
Clear trading backtesting structure following actual trading workflow.
"""

meta_data_loader = MetaclassMethodEnforcer(required_methods=["load_and_clean_data"], parent_class="DataLoader")
MetaDataLoaderEnforcer = meta_data_loader.enforce()


class DataLoader(metaclass=MetaDataLoaderEnforcer):
    @abstractmethod
    def load_and_clean_data(self) -> [pd.DataFrame, pl.DataFrame]:
        """Load and process/clean market data at basic level"""
        pass


meta_risk_manager = MetaclassMethodEnforcer(required_methods=["quantify_risk"], parent_class="RiskManager")
MetaRiskManagerEnforcer = meta_risk_manager.enforce()


class RiskManager(metaclass=MetaRiskManagerEnforcer):
    @abstractmethod
    def quantify_risk(self, df: [pd.DataFrame, pl.DataFrame]) -> (pd.DataFrame, pl.DataFrame):
        return df


meta_position_manager = MetaclassMethodEnforcer(
    required_methods=["detect_trade", "adjust_position"], parent_class="PositionManager"
)
MetaPositionManagerEnforcer = meta_position_manager.enforce()


class PositionManager(metaclass=MetaPositionManagerEnforcer):
    @abstractmethod
    def detect_trade(self, df: [pd.DataFrame, pl.DataFrame]) -> (pd.DataFrame, pl.DataFrame):
        """
        Detect trading opportunities ---> flag when to enter, exit, hedge, roll, spread, etc.
        """
        return df

    @abstractmethod
    def adjust_position(self, df: [pd.DataFrame, pl.DataFrame]) -> (pd.DataFrame, pl.DataFrame):
        """
        Adjust overall position by executing a trade or number of trades.
        """
        return df


meta_strategy_evaluator = MetaclassMethodEnforcer(required_methods=["evaluate_strategy"], parent_class="StrategyEvaluator")
MetaStrategyEvaluatorEnforcer = meta_strategy_evaluator.enforce()


class StrategyEvaluator(metaclass=MetaStrategyEvaluatorEnforcer):
    @abstractmethod
    def evaluate_strategy(self, df: [pd.DataFrame, pl.DataFrame]) -> (pd.DataFrame, pl.DataFrame):
        """
        Comprehensively evaluate the strategy to determine its robustness.
        """
        return df


class BacktestEngine:
    def __init__(
        self,
        data_loader: DataLoader,
        risk_manager: RiskManager,
        position_manager: PositionManager,
        strategy_evaluator: StrategyEvaluator,
    ):
        self.data_loader = data_loader
        self.risk_manager = risk_manager
        self.position_manager = position_manager
        self.strategy_evaluator = strategy_evaluator
        self.df_evaluation = None

    def load_and_clean_data(self) -> (pd.DataFrame, pl.DataFrame):
        self.df = self.data_loader.load_and_clean_data()

    def manage_risk(self) -> (pd.DataFrame, pl.DataFrame):
        # Quantify risk before deciding on position adjustments
        self.df = self.risk_manager.quantify_risk(self.df)

    def manage_positions(self) -> (pd.DataFrame, pl.DataFrame):
        # Detect and execute trades
        self.df = self.position_manager.detect_trade(self.df)
        self.df = self.position_manager.adjust_position(self.df)

    def evaluate_strategy(self) -> (pd.DataFrame, pl.DataFrame):
        # Evaluate the strategy
        self.df_evaluation = self.strategy_evaluator.evaluate_strategy(self.df)

    def run_backtest(self):
        """Run the full trading simulation"""
        self.load_and_clean_data()
        self.manage_risk()
        self.manage_positions()
        self.evaluate_strategy()
        return self
