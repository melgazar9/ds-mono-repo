import pandas as pd
import numpy as np

# import polars as pl


def calc_win_rate(series):
    wins = (series > 0).sum()
    total = series.notna().sum()
    return wins / total if total > 0 else 0


def calc_profit_factor(series):
    profits = series[series > 0].sum()
    losses = abs(series[series < 0].sum())
    return profits / losses if losses > 0 else (np.inf if profits > 0 else 0)


def calc_intraday_sharpe_ratio(
    returns: pd.Series,
    risk_free_rate: float = 0.0525,
    min_observations: int = 10,
    remove_outliers: bool = False,
    outlier_threshold: float = 3.0,
) -> float:
    if returns is None or len(returns) == 0:
        return np.nan

    clean_returns = returns.dropna()
    clean_returns = clean_returns[np.isfinite(clean_returns)]

    n_trades = len(clean_returns)

    if n_trades < min_observations:
        return np.nan

    if clean_returns.std() == 0:
        return 0.0 if clean_returns.mean() >= 0 else -np.inf

    if remove_outliers and n_trades > 20:  # Only remove outliers if sufficient data
        z_scores = np.abs((clean_returns - clean_returns.mean()) / clean_returns.std())
        clean_returns = clean_returns[z_scores <= outlier_threshold]

        # Recheck after outlier removal
        if len(clean_returns) < min_observations:
            return np.nan
        if clean_returns.std() == 0:
            return 0.0 if clean_returns.mean() >= 0 else -np.inf

    trading_days_per_year = 252
    daily_rf_rate = risk_free_rate / trading_days_per_year

    rf_rate_per_trade = daily_rf_rate / max(n_trades, 1)

    excess_returns = clean_returns - rf_rate_per_trade
    mean_excess_return = excess_returns.mean()
    volatility = excess_returns.std(ddof=1)

    sharpe_ratio = mean_excess_return / volatility

    if abs(sharpe_ratio) > 50:  # Suspiciously high Sharpe suggests data issues
        return np.nan

    return sharpe_ratio


def calc_max_drawdown(pnl_series):
    cumulative = pnl_series.cumsum()
    running_max = cumulative.expanding().max()
    drawdown = cumulative - running_max
    return drawdown.min()


def calc_sortino_ratio(
    returns: pd.Series,
    risk_free_rate: float = 0.0525,
    target_return: float = None,
    min_observations: int = 10,
    remove_outliers: bool = False,
    outlier_threshold: float = 3.0,
) -> float:
    """
    Calculate Sortino ratio with same rigor as Sharpe ratio
    """
    if returns is None or len(returns) == 0:
        return np.nan

    clean_returns = returns.dropna()
    clean_returns = clean_returns[np.isfinite(clean_returns)]

    n_trades = len(clean_returns)

    if n_trades < min_observations:
        return np.nan

    # Handle outlier removal (same as Sharpe)
    if remove_outliers and n_trades > 20:
        z_scores = np.abs((clean_returns - clean_returns.mean()) / clean_returns.std())
        clean_returns = clean_returns[z_scores <= outlier_threshold]

        # Recheck after outlier removal
        if len(clean_returns) < min_observations:
            return np.nan

    # Calculate risk-free rate per trade (same as Sharpe)
    trading_days_per_year = 252
    daily_rf_rate = risk_free_rate / trading_days_per_year
    rf_rate_per_trade = daily_rf_rate / max(n_trades, 1)

    # Use risk-free rate as target if not specified
    if target_return is None:
        target_return = rf_rate_per_trade

    # Calculate excess returns
    excess_returns = clean_returns - rf_rate_per_trade
    mean_excess_return = excess_returns.mean()

    # Calculate downside deviation (key difference from Sharpe)
    downside_returns = excess_returns[
        excess_returns < (target_return - rf_rate_per_trade)
    ]

    if len(downside_returns) == 0:
        # No downside - but still need to handle edge cases
        if mean_excess_return > 0:
            return np.inf
        else:
            return 0.0

    # Use sample standard deviation (ddof=1) for consistency with Sharpe
    downside_deviation = np.sqrt(np.mean(downside_returns ** 2))

    if downside_deviation == 0:
        return 0.0 if mean_excess_return >= 0 else -np.inf

    sortino_ratio = mean_excess_return / downside_deviation

    # Same sanity check as Sharpe
    if abs(sortino_ratio) > 50:
        return np.nan

    return sortino_ratio
