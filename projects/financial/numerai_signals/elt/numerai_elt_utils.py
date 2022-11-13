import pandas as pd

from ds_core.ds_utils import *
import yfinance as yf
import simplejson

def download_yf_prices(tickers,
                       intervals_to_download=['1d', '1h', '1m', '2m', '5m'],
                       num_workers=1,
                       n_chunks=1,
                       yf_params={},
                       yahoo_ticker_colname='yahoo_ticker',
                       verbose=True):
    """
    Parameters
    __________
    See yf.download docs for a detailed description of yf parameters
    tickers: list of tickers to pass to yf.download - it will be parsed to be in the format "AAPL MSFT FB"
    intervals_to_download : list of intervals to download OHLCV data for each stock (e.g. ['1w', '1d', '1h'])
    num_workers: number of threads used to download the data
        so far only 1 thread is implemented
    n_chunks: int number of chunks to pass to yf.download()
        1 is the slowest but most reliable because if two are passed and one fails, then both tickers are not returned
    tz_localize_location: timezone location to set the datetime
    **yf_params: dict - passed to yf.download(yf_params)
        set threads = True for faster performance, but tickers will fail, scipt may hang
        set threads = False for slower performance, but more tickers will succeed
    NOTE: passing some intervals return unreliable stock data (e.g. '3mo' returns many NAs when they should not be)
    """

    failed_ticker_downloads = []

    if 'start' not in yf_params.keys():
        if verbose:
            print('*** yf params start set to 2005-01-01! ***')
        yf_params['start'] = '2005-01-01'
    if 'threads' not in yf_params.keys() or not yf_params['threads']:
        if verbose:
            print('*** yf params threads set to False! ***')
        yf_params['threads'] = False
    if not verbose:
        yf_params['progress'] = False

    start_date = yf_params['start']
    assert pd.Timestamp(start_date) <= datetime.today(), 'Start date cannot be after the current date!'

    if num_workers == 1:
        dict_of_dfs = {}
        for i in intervals_to_download:
            print(f'\n*** Running interval {i} ***\n')
            yf_params['interval'] = i

            # Maximum lookback period for intervals i
            # 1m: 7 days
            # 2m: 60 days
            # 5m: 60 days
            # 15m: 60 days
            # 30m: 60 days
            # 60m: 730 days
            # 90m: 60 days
            # 1h: 730 days
            # 1d: 50+ years
            # 5d: 50+ years
            # 1wk: 50+ years
            # 1mo: 50+ years --- Buggy!
            # 3mo: 50+ years --- Buggy!

            if i == '1m':
                yf_params['start'] = datetime.today() - timedelta(days=6)
            elif i in ['2m', '5m', '15m', '30m', '90m']:
                yf_params['start'] = datetime.today() - timedelta(days=58)
            elif i in ['60m', '1h']:
                yf_params['start'] = datetime.today() - timedelta(days=728)
            else:
                yf_params['start'] = pd.to_datetime(yf_params['start']).strftime('%Y-%m-%d')

            if yf_params['threads'] == True:
                df_i = yf.download(' '.join(tickers), **yf_params)\
                               .stack()\
                               .rename_axis(index=['timestamp', yahoo_ticker_colname])\
                               .reset_index()
                if isinstance(df_i.columns, pd.MultiIndex):
                    df_i.columns = flatten_multindex_columns(df_i)
                df_i = clean_columns(df_i)
                dict_of_dfs[i] = df_i
            else:
                ticker_chunks = [' '.join(tickers[i:i+n_chunks]) for i in range(0, len(tickers), n_chunks)]
                chunk_dfs_lst = []
                column_order = clean_columns(
                    pd.DataFrame(
                        columns=['timestamp', yahoo_ticker_colname, 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
                    )
                ).columns.tolist()

                for chunk in ticker_chunks:
                    if verbose:
                        print(f"Running chunk {chunk}")
                    try:
                        if n_chunks == 1 or len(chunk.split(' ')) == 1:
                            try:
                                df_tmp = \
                                    yf.download(chunk, **yf_params)\
                                            .rename_axis(index='timestamp')\
                                            .pipe(lambda x: clean_columns(x))
                                df_tmp[yahoo_ticker_colname] = chunk
                                df_tmp.reset_index(inplace=True)
                                df_tmp = df_tmp[column_order]

                                if len(df_tmp) == 0:
                                    continue
                            except:
                                if verbose:
                                    print(f"failed download for tickers: {chunk}")
                                failed_ticker_downloads.append(chunk)
                                continue

                        else:
                            # should be the order of column_order
                            df_tmp = yf.download(chunk, **yf_params)\
                                .stack()\
                                .rename_axis(index=['timestamp', yahoo_ticker_colname])\
                                .reset_index()\
                                .pipe(lambda x: clean_columns(x))

                            if len(df_tmp) == 0:
                                continue

                            df_tmp = df_tmp[column_order]

                        chunk_dfs_lst.append(df_tmp)

                    except simplejson.errors.JSONDecodeError:
                        pass

                df_i = pd.concat(chunk_dfs_lst)
                dict_of_dfs[i] = df_i
                del chunk_dfs_lst
                yf_params['start'] = start_date

            ### print errors ###

            if verbose:
                if len(failed_ticker_downloads) > 0:
                    if n_chunks > 1:
                        failed_ticker_downloads = list(itertools.chain(*failed_ticker_downloads))

                print(f"\nFailed ticker downloads:\n{failed_ticker_downloads}")

    else:
        raise ValueError("Multi-threading not supported yet.")

    return dict_of_dfs


def download_ticker_map(napi,
                        numerai_ticker_link='https://numerai-signals-public-data.s3-us-west-2.amazonaws.com/signals_ticker_map_w_bbg.csv',
                        main_ticker_col='bloomberg_ticker',
                        yahoo_ticker_colname='yahoo',
                        verbose=True):

    eligible_tickers = pd.Series(napi.ticker_universe(), name=yahoo_ticker_colname)
    ticker_map = pd.read_csv(numerai_ticker_link)
    ticker_map = ticker_map[ticker_map[main_ticker_col].isin(eligible_tickers)]

    if verbose:
        print(f"Number of eligible tickers: {len(eligible_tickers)}")
        print(f"Number of eligible tickers in map: {len(ticker_map)}")

    # Remove null / empty tickers from the yahoo tickers
    valid_tickers = [i for i in ticker_map[yahoo_ticker_colname]
                     if not pd.isnull(i)
                     and not str(i).lower() == 'nan'\
                     and not str(i).lower() == 'null'\
                     and i is not None\
                     and not str(i).lower() == ''\
                     and len(i) > 0]

    if verbose:
        print('tickers before cleaning:', ticker_map.shape)  # before removing bad tickers

    ticker_map = ticker_map[ticker_map[yahoo_ticker_colname].isin(valid_tickers)]

    if verbose:
        print('tickers after cleaning:', ticker_map.shape)

    return ticker_map
