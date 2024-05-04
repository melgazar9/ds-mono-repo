from ds_core.ds_utils import *
from ds_core.db_connectors import *


class TargetCreator:
    pass


class MarketCondition:
    def __init__(
        self,
        df,
        open_col="open",
        high_col="high",
        low_col="low",
        close_col="close",
        volume_col="volume",
        make_copy=True,
    ):
        self.df = df if not make_copy else df.copy()
        self.open_col = open_col
        self.high_col = high_col
        self.low_col = low_col
        self.close_col = close_col
        self.volume_col = volume_col
        self.make_copy = make_copy

    def calc_pct_below_all_time_high(self):
        self.df.loc[:, "all_time_high"] = self.df[self.high_col].expanding().max()
        self.df.loc[:, "pct_below_all_time_high"] = (
            self.df[self.high_col] / self.df["all_time_high"]
        )
        return self

    def calc_market_condition(
        self,
        pct_below_ath_correction=0.10,
        pct_below_ath_bear=0.20,
        above_recent_low_pct=0.20,
        trailing_recent_low_days=90,
    ):
        self.calc_pct_below_all_time_high()

        self.df.loc[
            self.df["pct_below_all_time_high"] <= (1 - pct_below_ath_correction),
            "market_condition",
        ] = "correction"

        self.df.loc[
            self.df["pct_below_all_time_high"] <= (1 - pct_below_ath_bear),
            "market_condition",
        ] = "bear"

        self.df.loc[:, f"recent_low_{trailing_recent_low_days}_days"] = (
            self.df["low"].rolling(window=trailing_recent_low_days).min().bfill()
        )

        self.df.loc[
            (
                (
                    self.df["close"]
                    > self.df[f"recent_low_{trailing_recent_low_days}_days"]
                    * (1 + above_recent_low_pct)
                )
                | (self.df["pct_below_all_time_high"] > (1 - pct_below_ath_correction))
            )
            & (
                (
                    self.df["close"].shift(-30)
                    > self.df[f"recent_low_{trailing_recent_low_days}_days"]
                    * (1 + above_recent_low_pct)
                )
                & (
                    self.df["close"].shift(-60)
                    > self.df[f"recent_low_{trailing_recent_low_days}_days"]
                    * (1 + above_recent_low_pct)
                )
            ),
            "market_condition",
        ] = "bull"

        self.df["market_condition"] = self.df["market_condition"].ffill().bfill()

        self.df["bull_or_bear_market"] = (
            self.df["market_condition"].replace("correction", np.nan).ffill()
        )

        return self

    def calc_sp500_tick(self, df):
        pass

    def calc_dow_jones_tick(self, df):
        pass

    def calc_nasdaq100_tick(self, df):
        pass
