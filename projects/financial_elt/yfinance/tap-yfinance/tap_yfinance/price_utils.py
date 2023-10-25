from datetime import datetime, timedelta
import time
import logging
import pandas as pd
import numpy as np
import yfinance as yf
from skimpy import clean_columns
from pytickersymbols import PyTickerSymbols
from numerapi import SignalsAPI
from pandas_datareader import data as pdr

class YFinanceLogger:
    """ Logger inherited by all YFinanceTap classes """

    def __init__(self, log_level=logging.INFO):
        self.log_level = log_level

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(self.log_level)
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s  %(name)s:  %(levelname)s  %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

class YFinancePriceTap(YFinanceLogger):
    """
    Parameters
    ----------
    yf_params: dict - passed to yf.Ticker(<ticker>).history(**yf_params) - see docs for yfinance ticker.history() params
    ticker_colname: str of column name to set of output yahoo ticker columns
    """

    def __init__(self,
                 schema_category,
                 yf_params=None,
                 ticker_colname='ticker'):
        self.schema_category = schema_category
        self.yf_params = {} if yf_params is None else yf_params
        self.ticker_colname = ticker_colname

        super().__init__()

        if 'prepost' not in self.yf_params.keys():
            self.yf_params['prepost'] = True
        if 'start' not in self.yf_params.keys():
            self.yf_params['start'] = '1950-01-01'

        self.start_date = self.yf_params['start']

        assert pd.Timestamp(self.start_date) <= datetime.today(), 'Start date cannot be after the current date!'

        assert 'stock_prices' in self.schema_category or 'forex_prices' in self.schema_category or 'crypto_prices' in self.schema_category, \
            "self.schema_category must be set to either 'stock_prices', 'forex_prices', or 'crypto_prices'"

        if self.schema_category == 'stock_prices':
            self.column_order = ['timestamp', 'timestamp_tz_aware', 'timezone', 'ticker', 'open', 'high', 'low',
                                 'close', 'volume', 'dividends', 'stock_splits', 'repaired']

        elif schema_category == 'forex_prices':
            self.column_order = ['timestamp', 'timestamp_tz_aware', 'timezone', 'ticker', 'open', 'high', 'low',
                                 'close', 'volume', 'repaired']

        elif schema_category == 'crypto_prices':
            self.column_order = ['timestamp', 'timestamp_tz_aware', 'timezone', 'ticker', 'open', 'high', 'low',
                                 'close', 'volume', 'repaired']
        elif schema_category.endswith('wide'):
            self.column_order = None
        else:
            raise ValueError('Could not determine price column order.')

        self.n_requests = 0

        self.failed_ticker_downloads = {}

    def _request_limit_check(self):
        """
        Description
        -----------
        Check if too many requests were made to yfinance within their allowed number of requests.
        """

        self.request_start_timestamp = datetime.now()
        self.current_runtime_seconds = (datetime.now() - self.request_start_timestamp).seconds

        if self.n_requests > 1900 and self.current_runtime_seconds > 3500:
            self.logger.info(f'\nToo many requests per hour. Pausing requests for {self.current_runtime_seconds} seconds.\n')
            time.sleep(np.abs(3600 - self.current_runtime_seconds))
        if self.n_requests > 45000 and self.current_runtime_seconds > 85000:
            self.logger.info(f'\nToo many requests per day. Pausing requests for {self.current_runtime_seconds} seconds.\n')
            time.sleep(np.abs(86400 - self.current_runtime_seconds))
        return self

    def download_price_history(self, ticker, yf_params=None) -> pd.DataFrame():
        """
        Description
        -----------
        Download a single stock price ticker from the yfinance python library.
        Minor transformations happen:
            - Add column ticker to show which ticker has been pulled
            - Set start date to the minimum start date allowed by yfinance for that ticker (passed in yf_params)
            - Clean column names
            - Set tz_aware timestamp column to be a string
        """
        yf_params = self.yf_params.copy() if yf_params is None else yf_params.copy()

        assert 'interval' in yf_params.keys(), 'must pass interval parameter to yf_params'

        if yf_params['interval'] not in self.failed_ticker_downloads.keys():
            self.failed_ticker_downloads[yf_params['interval']] = []

        if 'start' not in yf_params.keys():
            yf_params['start'] = '1950-01-01 00:00:00'
            self.logger.info(f'\n*** YF params start set to 1950-01-01 for ticker {ticker}! ***\n')

        yf_params['start'] = \
            get_valid_yfinance_start_timestamp(interval=yf_params['interval'], start=yf_params['start'])

        t = yf.Ticker(ticker)
        try:
            df = \
                t.history(**yf_params) \
                    .rename_axis(index='timestamp') \
                    .pipe(lambda x: clean_columns(x))

            self.n_requests += 1
            df.loc[:, self.ticker_colname] = ticker
            df = df.reset_index()
            df.loc[:, 'timestamp_tz_aware'] = df['timestamp'].copy()
            df.loc[:, 'timezone'] = str(df['timestamp_tz_aware'].dt.tz)
            df['timestamp_tz_aware'] = df['timestamp_tz_aware'].dt.strftime('%Y-%m-%d %H:%M:%S%z')
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

            if df is not None and not df.shape[0]:
                self.failed_ticker_downloads[yf_params['interval']].append(ticker)
                return pd.DataFrame(columns=self.column_order)

            df = df.replace([np.inf, -np.inf, np.nan], None)  # None can be handled by json.dumps but inf and NaN can't be
            df = df[self.column_order]
            return df

        except:
            self.failed_ticker_downloads[yf_params['interval']].append(ticker)
            return pd.DataFrame(columns=self.column_order)

    def download_price_history_wide(self, tickers, yf_params):
        yf_params = self.yf_params.copy() if yf_params is None else yf_params.copy()

        assert 'interval' in yf_params.keys(), 'must pass interval parameter to yf_params'

        if yf_params['interval'] not in self.failed_ticker_downloads.keys():
            self.failed_ticker_downloads[yf_params['interval']] = []

        if 'start' not in yf_params.keys():
            yf_params['start'] = '1950-01-01 00:00:00'
            self.logger.info(f'\n*** YF params start set to 1950-01-01 for ticker {ticker}! ***\n')

        yf_params['start'] = \
            get_valid_yfinance_start_timestamp(interval=yf_params['interval'], start=yf_params['start'])

        yf.pdr_override()

        df = pdr.get_data_yahoo(tickers, progress=False, **yf_params).rename_axis(index='timestamp').reset_index()
        self.n_requests += 1

        df.columns = flatten_multindex_columns(df, clean_columns_names=True)
        df.loc[:, 'timestamp_tz_aware'] = df['timestamp'].copy()
        df.loc[:, 'timezone'] = str(df['timestamp_tz_aware'].dt.tz)
        df['timestamp_tz_aware'] = df['timestamp_tz_aware'].dt.strftime('%Y-%m-%d %H:%M:%S%z')
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

        if df is not None and not df.shape[0]:
            self.failed_ticker_downloads[yf_params['interval']].append(ticker)
            return pd.DataFrame(columns=self.column_order)

        df = df.replace([np.inf, -np.inf, np.nan], None)
        return df


class TickerDownloader(YFinanceLogger):
    """
    Description
    -----------
    Class to download PyTickerSymbols, Yahoo, and Numerai ticker symbols into a single dataframe.
    A mapping between all symbols is returned when calling the method download_valid_stock_tickers().
    """

    def __init__(self):
        super().__init__()

    @staticmethod
    def download_pts_stock_tickers():
        """
        Description
        -----------
        Download py-ticker-symbols tickers
        """
        pts = PyTickerSymbols()
        all_getters = list(filter(
            lambda x: (
                    x.endswith('_yahoo_tickers') or x.endswith('_google_tickers')
            ),
            dir(pts),
        ))

        all_tickers = {'yahoo_tickers': [], 'google_tickers': []}
        for t in all_getters:
            if t.endswith('google_tickers'):
                all_tickers['google_tickers'].append((getattr(pts, t)()))
            elif t.endswith('yahoo_tickers'):
                all_tickers['yahoo_tickers'].append((getattr(pts, t)()))
        all_tickers['google_tickers'] = flatten_list(all_tickers['google_tickers'])
        all_tickers['yahoo_tickers'] = flatten_list(all_tickers['yahoo_tickers'])
        if len(all_tickers['yahoo_tickers']) == len(all_tickers['google_tickers']):
            all_tickers = pd.DataFrame(all_tickers)
        else:
            all_tickers = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in all_tickers.items()]))

        all_tickers = \
            all_tickers \
                .rename(columns={'yahoo_tickers': 'yahoo_ticker', 'google_tickers': 'google_ticker'}) \
                .sort_values(by=['yahoo_ticker', 'google_ticker']) \
                .drop_duplicates()
        all_tickers = all_tickers.replace([-np.inf, np.inf, np.nan], None)

        return all_tickers

    @staticmethod
    def download_top_250_crypto_tickers(num_currencies=250):
        """
        Description
        -----------
        Download the top 250 cryptocurrencies
        Note: At the time of coding, setting num_currencies higher than 250 results in only 25 crypto tickers returned.
        """

        from requests_html import HTMLSession

        session = HTMLSession()
        resp = session.get(f"https://finance.yahoo.com/crypto?offset=0&count={num_currencies}")
        tables = pd.read_html(resp.html.raw_html)
        session.close()

        df = clean_columns(tables[0].copy())
        df.rename(columns={'symbol': 'yahoo_ticker', 'name': 'yahoo_name', '%_change': 'pct_change'}, inplace=True)

        # Add Decentral-Games tickers

        missing_dg_tickers = [i for i in ['ICE13133-USD', 'DG15478-USD', 'XDG-USD'] if i not in df['yahoo_ticker']]
        if len(missing_dg_tickers):
            df_dg = pd.DataFrame({
                'yahoo_ticker': missing_dg_tickers,
                'yahoo_name': missing_dg_tickers,
                'price_intraday': np.nan,
                'change': np.nan,
                'pct_change': np.nan,
                'market_cap': np.nan,
                'volume_in_currency_since_0_00_utc': np.nan,
                'volume_in_currency_24_hr': np.nan,
                'total_volume_all_currencies_24_hr': np.nan,
                'circulating_supply': np.nan,
                '52_week_range': np.nan,
                'day_chart': np.nan
            })

            df = pd.concat([df, df_dg], axis=0).reset_index(drop=True)

        df = df.dropna(how='all', axis=1)
        df = df.replace([np.inf, -np.inf, np.nan], None)
        return df

    @staticmethod
    def download_forex_pairs():
        forex_pairs = dict(
            yahoo_ticker=[
                'EURUSD=X', 'JPY=X', 'GBPUSD=X', 'AUDUSD=X', 'NZDUSD=X', 'EURJPY=X', 'GBPJPY=X', 'EURGBP=X',
                'EURCAD=X', 'EURSEK=X', 'EURCHF=X', 'EURHUF=X', 'CNY=X', 'HKD=X', 'SGD=X', 'INR=X', 'MXN=X',
                'PHP=X', 'IDR=X', 'THB=X', 'MYR=X', 'ZAR=X', 'RUB=X'
            ],
            yahoo_name=[
                'USD/EUR', 'USD/JPY', 'USD/GBP', 'USD/AUD', 'USD/NZD', 'EUR/JPY', 'GBP/JPY', 'EUR/GBP',
                'EUR/CAD', 'EUR/SEK', 'EUR/CHF', 'EUR/HUF', 'USD/CNY', 'USD/HKD', 'USD/SGD', 'USD/INR', 'USD/MXN',
                'USD/PHP', 'USD/IDR', 'USD/THB', 'USD/MYR', 'USD/ZAR', 'USD/RUB'
            ]
        )

        # TODO: Download a list of all relevant forex pairs
        df_forex_pairs = pd.DataFrame(forex_pairs)
        df_forex_pairs.loc[:, 'bloomberg_ticker'] = df_forex_pairs['yahoo_name'].apply(lambda x: f"{x[4:]}-{x[0:3]}")
        df_forex_pairs = df_forex_pairs.replace([np.inf, -np.inf, np.nan], None)

        return df_forex_pairs

    def download_numerai_signals_ticker_map(
            self,
            napi=SignalsAPI(),
            numerai_ticker_link='https://numerai-signals-public-data.s3-us-west-2.amazonaws.com/signals_ticker_map_w_bbg.csv',
            yahoo_ticker_colname='yahoo'):

        """  Download numerai to yahoo ticker mapping  """

        ticker_map = pd.read_csv(numerai_ticker_link)
        eligible_tickers = pd.Series(napi.ticker_universe(), name='bloomberg_ticker')
        ticker_map = pd.merge(ticker_map, eligible_tickers, on='bloomberg_ticker', how='right')

        self.logger.info('Number of eligible tickers in map: %s', str(ticker_map.shape[0]))

        ticker_map = ticker_map.replace([np.inf, -np.inf, np.nan], None)
        valid_tickers = [i for i in ticker_map[yahoo_ticker_colname] if i is not None and len(i) > 0]

        self.logger.info(f'tickers before cleaning: %s', ticker_map.shape)
        ticker_map = ticker_map[ticker_map[yahoo_ticker_colname].isin(valid_tickers)]
        self.logger.info(f'tickers after cleaning: %s', ticker_map.shape)
        return ticker_map

    @classmethod
    def download_valid_stock_tickers(cls):
        """
        Description
        -----------
        Download the valid tickers from py-ticker-symbols
        """

        # napi = numerapi.SignalsAPI(os.environ.get('NUMERAI_PUBLIC_KEY'), os.environ.get('NUMERAI_PRIVATE_KEY'))

        df_pts_tickers = cls.download_pts_stock_tickers()

        numerai_yahoo_tickers = \
            cls().download_numerai_signals_ticker_map() \
                .rename(columns={'yahoo': 'yahoo_ticker', 'ticker': 'numerai_ticker'})

        df1 = pd.merge(df_pts_tickers, numerai_yahoo_tickers, on='yahoo_ticker', how='left').set_index('yahoo_ticker')
        df2 = pd.merge(numerai_yahoo_tickers, df_pts_tickers, on='yahoo_ticker', how='left').set_index('yahoo_ticker')
        df3 = pd.merge(df_pts_tickers, numerai_yahoo_tickers, left_on='yahoo_ticker', right_on='numerai_ticker',
                       how='left') \
            .rename(columns={'yahoo_ticker_x': 'yahoo_ticker', 'yahoo_ticker_y': 'yahoo_ticker_old'}) \
            .set_index('yahoo_ticker')

        df4 = pd.merge(df_pts_tickers, numerai_yahoo_tickers, left_on='yahoo_ticker', right_on='bloomberg_ticker',
                       how='left') \
            .rename(columns={'yahoo_ticker_x': 'yahoo_ticker', 'yahoo_ticker_y': 'yahoo_ticker_old'}) \
            .set_index('yahoo_ticker')

        df_tickers_wide = clean_columns(pd.concat([df1, df2, df3, df4], axis=1))

        for col in df_tickers_wide.columns:
            suffix = col[-1]
            if suffix.isdigit():
                root_col = col.strip('_' + suffix)
                df_tickers_wide.loc[:, root_col] = df_tickers_wide[root_col].fillna(df_tickers_wide[col])

        df_tickers = \
            df_tickers_wide.reset_index() \
                [['yahoo_ticker', 'google_ticker', 'bloomberg_ticker', 'numerai_ticker', 'yahoo_ticker_old']] \
                .sort_values(
                by=['yahoo_ticker', 'google_ticker', 'bloomberg_ticker', 'numerai_ticker', 'yahoo_ticker_old']) \
                .drop_duplicates()

        df_tickers.loc[:, 'yahoo_valid_pts'] = False
        df_tickers.loc[:, 'yahoo_valid_numerai'] = False

        df_tickers.loc[
            df_tickers['yahoo_ticker'].isin(df_pts_tickers['yahoo_ticker'].tolist()), 'yahoo_valid_pts'] = True

        df_tickers.loc[
            df_tickers['yahoo_ticker'].isin(numerai_yahoo_tickers['numerai_ticker'].tolist()), 'yahoo_valid_numerai'
        ] = True

        df_tickers = df_tickers.replace([np.inf, -np.inf, np.nan], None)

        return df_tickers

def get_valid_yfinance_start_timestamp(interval, start='1950-01-01 00:00:00'):
    """
    Description
    -----------
    Get a valid yfinance date to lookback

    Valid intervals with maximum lookback period
    1m: 7 days
    2m: 60 days
    5m: 60 days
    15m: 60 days
    30m: 60 days
    60m: 730 days
    90m: 60 days
    1h: 730 days
    1d: 50+ years
    5d: 50+ years
    1wk: 50+ years
    1mo: 50+ years --- Buggy!
    3mo: 50+ years --- Buggy!

    Note: Often times yfinance returns an error even when looking back maximum number of days - 1,
        by default, return a date 2 days closer to the current date than the maximum specified in the yfinance docs

    """

    valid_intervals = ['1m', '2m', '5m', '15m', '30m', '60m', '1h', '90m', '1d', '5d', '1wk', '1mo', '3mo']
    assert interval in valid_intervals, f'must pass a valid interval {valid_intervals}'

    if interval == '1m':
        updated_start = max((datetime.today() - timedelta(days=5)).date(), pd.to_datetime(start).date())
    elif interval in ['2m', '5m', '15m', '30m', '90m']:
        updated_start = (max((datetime.today() - timedelta(days=58)).date(), pd.to_datetime(start).date()))
    elif interval in ['60m', '1h']:
        updated_start = max((datetime.today() - timedelta(days=728)).date(), pd.to_datetime(start).date())
    else:
        updated_start = pd.to_datetime(start)
    updated_start = updated_start.strftime('%Y-%m-%d')  # yfinance doesn't like strftime with hours, minutes, or seconds
    return updated_start

def flatten_list(lst):
    return [v for item in lst for v in (item if isinstance(item, list) else [item])]

def flatten_multindex_columns(df, clean_columns_names=False):
    new_cols = list(
        pd.Index([str(e[0]).lower() + '_' + str(e[1]).lower() for e in df.columns.tolist()]).str.replace(' ', '_')
    )

    if clean_columns_names:
        return list(clean_columns(pd.DataFrame(columns=new_cols)))

    return new_cols