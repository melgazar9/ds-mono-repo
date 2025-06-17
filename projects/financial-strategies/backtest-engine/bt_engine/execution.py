import pandas as pd
import polars as pl
from typing import Union


def apply_slippage(
    df: Union[pd.DataFrame, pl.DataFrame],
    price_col: str,
    slippage_amount: float,
    *,
    slippage_mode: str,
    side: str,
    high_col: str = "high",
    low_col: str = "low"
) -> pd.Series:
    """
    Apply slippage to opening and closing trades.

    - df: DataFrame containing price, high, and low columns.
    - price_col: Name of the column representing theoretical prices (buy or sell).
    - slippage_amount:
        * If slippage_mode == 'fixed_pct', it's a decimal percentage (e.g., 0.001 for 0.1%).
        * If slippage_mode == 'partway', it's a fraction between 0 and 1.
    - slippage_mode: "fixed_pct" or "partway".
    - side: "buy" or "sell".
    - high_col, low_col: Column names for high/low. Defaults: "high"/"low".
    """
    price = df[price_col]
    high = df[high_col]
    low = df[low_col]

    if slippage_mode == "fixed_pct":
        offset = price * slippage_amount
        if side == "buy":
            return price + offset
        else:  # sell
            return price - offset
    elif slippage_mode == "partway":
        if side == "buy":
            return low + slippage_amount * (high - low)
        else:  # sell
            return high - slippage_amount * (high - low)
    else:
        raise ValueError("slippage_mode must be 'fixed_pct' or 'partway'")
