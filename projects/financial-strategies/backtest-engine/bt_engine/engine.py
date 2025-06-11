from abc import ABC, abstractmethod
import pandas as pd
import polars as pl

"""
Clear trading backtesting structure following actual trading workflow.
"""


class DataLoader(ABC):
    @abstractmethod
    def load_and_clean_data(self) -> [pd.DataFrame, pl.DataFrame]:
        """Load and process/clean market data at basic level"""
        pass


class PositionManager(ABC):
    @abstractmethod
    def detect_trade(
        self, df: [pd.DataFrame, pl.DataFrame]
    ) -> (pd.DataFrame, pl.DataFrame):
        """
        Detect trading opportunities ---> flag when to enter, exit, hedge, roll, spread, etc.
        """
        pass

    @abstractmethod
    def execute_trade(
        self, df: [pd.DataFrame, pl.DataFrame]
    ) -> (pd.DataFrame, pl.DataFrame):
        """
        Execute trade based on the result of self.detect_trade.
        While detect_trade returns flag or string of the trade, execute_trade should update the df to contain
        necessary columns that change the position of the trade
        """
        pass


class StrategyEvaluator:
    @abstractmethod
    def evaluate(
        self, df: [pd.DataFrame, pl.DataFrame]
    ) -> (pd.DataFrame, pl.DataFrame):
        """
        Comprehensively evaluate the strategy to determine how robust it is.
        """
        pass


class BacktestEngine:
    def __init__(self, data_loader: DataLoader):
        self.data_loader = data_loader
        self.position_manager = None
        self.strategy_evaluator = None
        self.df_evaluation = None

    def run_backtest(self, position_manager, strategy_evaluator):
        """Run the full trading simulation"""

        self.df = self.data_loader.load_and_clean_data()
        self.position_manager = position_manager()
        self.strategy_evaluator = strategy_evaluator()

        # Detect and execute trades
        self.df = self.position_manager.detect_trade(self.df)
        self.df = self.position_manager.execute_trade(self.df)

        # Evaluate the strategy
        self.df_evaluation = self.strategy_evaluator.evaluate(self.df)

        return self
