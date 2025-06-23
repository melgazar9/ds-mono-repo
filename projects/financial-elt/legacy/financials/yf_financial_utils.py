from yf_elt_stock_price_utils import *


class YFinanceFinancialsGetter:
    def __init__(self, tickers):
        self.tickers = [tickers] if isinstance(tickers, str) else tickers

        assert isinstance(tickers, (tuple, list)), "parameter tickers must be in the format str, list, or tuple"

        self.financials_to_get = (
            "actions",
            "analysis",
            "balance_sheet",
            "calendar",
            "cashflow",
            "dividends",
            "earnings",
            "earnings_dates",
            "financials",
            "institutional_holders",
            "major_holders",
            "mutualfund_holders",
            "quarterly_balance_sheet",
            "quarterly_cashflow",
            "quarterly_earnings",
            "quarterly_financials",
            "recommendations",
            "shares",
            "splits",
            "sustainability",
        )

        self.dict_of_financials = dict()

        for f in self.financials_to_get:
            self.dict_of_financials[f] = pd.DataFrame()

    def get_financials(self):
        for ticker in self.tickers:
            t = yf.Ticker(ticker)

            self.dict_of_financials["actions"] = pd.concat(
                [
                    self.dict_of_financials["actions"],
                    t.actions.reset_index().assign(ticker=ticker).pipe(lambda x: clean_columns(x)),
                ]
            )

            self.dict_of_financials["analysis"] = pd.concat(
                [
                    self.dict_of_financials["analysis"],
                    t.analysis.reset_index().assign(ticker=ticker).pipe(lambda x: clean_columns(x)),
                ]
            )

            self.dict_of_financials["balance_sheet"] = pd.concat(
                [
                    self.dict_of_financials["balance_sheet"],
                    t.balance_sheet.T.rename_axis(index="date")
                    .reset_index()
                    .assign(ticker=ticker)
                    .pipe(lambda x: clean_columns(x)),
                ]
            )

            self.dict_of_financials["calendar"] = pd.concat(
                [self.dict_of_financials["calendar"], t.calendar.T.assign(ticker=ticker).pipe(lambda x: clean_columns(x))]
            )

            self.dict_of_financials["cashflow"] = pd.concat(
                [
                    self.dict_of_financials["cashflow"],
                    t.cashflow.T.rename_axis(index="date").reset_index().assign(ticker=ticker).pipe(lambda x: clean_columns(x)),
                ]
            )

            self.dict_of_financials["dividends"] = pd.concat(
                [
                    self.dict_of_financials["dividends"],
                    t.dividends.reset_index().assign(ticker=ticker).pipe(lambda x: clean_columns(x)),
                ]
            )

            self.dict_of_financials["earnings"] = pd.concat(
                [
                    self.dict_of_financials["earnings"],
                    t.earnings.reset_index().assign(ticker=ticker).pipe(lambda x: clean_columns(x)),
                ]
            )

            self.dict_of_financials["earnings_dates"] = pd.concat(
                [
                    self.dict_of_financials["earnings_dates"],
                    t.earnings_dates.rename(columns={"Surprise(%)": "surprise_pct"})
                    .reset_index()
                    .assign(ticker=ticker)
                    .pipe(lambda x: clean_columns(x)),
                ]
            )

            self.dict_of_financials["financials"] = pd.concat(
                [
                    self.dict_of_financials["financials"],
                    t.financials.T.rename_axis(index="date")
                    .reset_index()
                    .assign(ticker=ticker)
                    .pipe(lambda x: clean_columns(x)),
                ]
            )

            self.dict_of_financials["institutional_holders"] = pd.concat(
                [
                    self.dict_of_financials["institutional_holders"],
                    t.institutional_holders.rename(columns={"% Out": "pct_out"})
                    .assign(ticker=ticker)
                    .pipe(lambda x: clean_columns(x)),
                ]
            )

            self.dict_of_financials["major_holders"] = pd.concat(
                [
                    self.dict_of_financials["major_holders"],
                    t.major_holders.rename(columns={0: "pct", 1: "metadata"})
                    .assign(ticker=ticker)
                    .pipe(lambda x: clean_columns(x)),
                ]
            )

            self.dict_of_financials["mutualfund_holders"] = pd.concat(
                [
                    self.dict_of_financials["mutualfund_holders"],
                    t.mutualfund_holders.rename(columns={"% Out": "pct_out"})
                    .assign(ticker=ticker)
                    .pipe(lambda x: clean_columns(x)),
                ]
            )

            self.dict_of_financials["quarterly_balance_sheet"] = pd.concat(
                [
                    self.dict_of_financials["quarterly_balance_sheet"],
                    t.quarterly_balance_sheet.T.rename_axis(index="date")
                    .reset_index()
                    .assign(ticker=ticker)
                    .pipe(lambda x: clean_columns(x)),
                ]
            )

            self.dict_of_financials["quarterly_cashflow"] = pd.concat(
                [
                    self.dict_of_financials["quarterly_cashflow"],
                    t.quarterly_cashflow.T.rename_axis(index="date")
                    .reset_index()
                    .assign(ticker=ticker)
                    .pipe(lambda x: clean_columns(x)),
                ]
            )

            self.dict_of_financials["quarterly_earnings"] = pd.concat(
                [
                    self.dict_of_financials["quarterly_earnings"],
                    t.quarterly_earnings.reset_index().assign(ticker=ticker).pipe(lambda x: clean_columns(x)),
                ]
            )

            self.dict_of_financials["quarterly_financials"] = pd.concat(
                [
                    self.dict_of_financials["quarterly_financials"],
                    t.quarterly_financials.T.rename_axis(index="date")
                    .reset_index()
                    .assign(ticker=ticker)
                    .pipe(lambda x: clean_columns(x)),
                ]
            )

            self.dict_of_financials["recommendations"] = pd.concat(
                [
                    self.dict_of_financials["recommendations"],
                    t.recommendations.reset_index().assign(ticker=ticker).pipe(lambda x: clean_columns(x)),
                ]
            )

            self.dict_of_financials["shares"] = pd.concat(
                [
                    self.dict_of_financials["shares"],
                    t.shares.reset_index().assign(ticker=ticker).pipe(lambda x: clean_columns(x)),
                ]
            )

            self.dict_of_financials["sustainability"] = pd.concat(
                [
                    self.dict_of_financials["sustainability"],
                    t.sustainability.T.rename_axis(index="date")
                    .reset_index()
                    .assign(ticker=ticker)
                    .pipe(lambda x: clean_columns(x)),
                ]
            )
