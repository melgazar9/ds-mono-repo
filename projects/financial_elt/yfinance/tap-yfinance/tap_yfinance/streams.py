from __future__ import annotations

from abc import ABC

import pandas as pd

from singer_sdk import Tap
from typing import Iterable, Optional, Any
from singer_sdk.streams import Stream
from tap_yfinance.price_utils import *
from tap_yfinance.schema import *
from singer_sdk.helpers._state import increment_state


class BaseStream(Stream, ABC):
    def __init__(self, tap: Tap, catalog_entry: dict) -> None:
        self.catalog_entry = catalog_entry
        self.table_name = self.catalog_entry['table_name']
        self.financial_category = self.catalog_entry['metadata'][-1]['metadata']['schema-name']

        super().__init__(
            tap=tap,
            schema=self.catalog_entry['schema'],
            name=self.catalog_entry["table_name"]
        )

        self.stream_params = self.config.get('financial_category').get(self.financial_category).get(self.name)
        self.schema_category = self.stream_params.get('schema_category')

        self.tickers = None
        self.df_tickers = None
        self._ticker_download_calls = 0

    def get_schema(self):
        return get_schema(self.schema_category)

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    @property
    def partitions(self) -> list[dict]:
        if self._ticker_download_calls == 0:
            self.logger.info(f"Tickers have not been downloaded yet. Downloading now...")
            self.download_tickers(self.stream_params)

        assert isinstance(self.tickers, list), f'self.tickers must be a list, but it is of type {type(self.tickers)}.'

        return [{'ticker': t} for t in self.tickers]

    def download_tickers(self, stream_params):
        assert self._ticker_download_calls == 0, \
            f"self._ticker_download_calls should be set to 0 but is {self._ticker_download_calls}."

        ticker_downloader = TickerDownloader()

        if stream_params['tickers'] == '*':
            ticker_download_method = self.get_ticker_download_method()
            self.df_tickers = getattr(ticker_downloader, ticker_download_method)()
            self.tickers = self.df_tickers['yahoo_ticker'].unique().tolist()
        else:
            self.df_tickers = pd.DataFrame({'yahoo_ticker': stream_params['tickers']})
            self.tickers = stream_params['tickers']

        self._ticker_download_calls += 1

        assert self._ticker_download_calls == 1, \
            f"""
                self.download_tickers has been called too many times.
                It should only be called once but has been called {self._ticker_download_calls} times.
            """

        assert isinstance(self.tickers, list)
        return self

    def get_ticker_download_method(self):
        if self.catalog_entry['tap_stream_id'].startswith('stock'):
            return 'download_valid_stock_tickers'
        elif self.catalog_entry['tap_stream_id'].startswith('forex'):
            return 'download_forex_pairs'
        elif self.catalog_entry['tap_stream_id'].startswith('crypto'):
            return 'download_top_250_crypto_tickers'
        else:
            raise ValueError('Could not determine ticker_download_method')

class TickerStream(BaseStream):
    replication_key = "yahoo_ticker"
    is_timestamp_replication_key = False

    def __init__(self, tap: Tap, catalog_entry: dict) -> None:
        super().__init__(tap, catalog_entry)

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """
        Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.

        Args:
            context: Stream partition or context dictionary.
        """

        state = self.get_context_state(context)
        record = self.df_tickers[self.df_tickers['yahoo_ticker'] == context['ticker']].to_dict(orient='records')[0]
        increment_state(
            state,
            replication_key=self.replication_key,
            latest_record=record,
            is_sorted=self.is_sorted,
            check_sorted=self.check_sorted
        )
        yield record

class PriceStream(BaseStream):
    replication_key = "timestamp"
    is_timestamp_replication_key = True

    def __init__(self, tap: Tap, catalog_entry: dict) -> None:
        super().__init__(tap, catalog_entry)
        self.yf_params = self.stream_params.get('yf_params')
        self.price_tap = YFinancePriceTap(schema_category=self.schema_category)

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """
        Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.

        Args:
            context: Stream partition or context dictionary.

        """

        self.logger.info(f"\n\n\n*** Running ticker {context['ticker']} *** \n\n\n")
        yf_params = self.yf_params.copy()
        state = self.get_context_state(context)

        if state and 'progress_markers' in state.keys():
            start_date = \
                datetime.fromisoformat(state.get('progress_markers').get('replication_key_value')).strftime('%Y-%m-%d')
        else:
            start_date = self.config.get('default_start_date')

        yf_params['start'] = start_date

        df = self.price_tap.download_price_history(ticker=context['ticker'], yf_params=yf_params)

        for record in df.to_dict(orient='records'):
            replication_key = context['ticker'] + '|' + record['timestamp'].strftime('%Y-%m-%d %H:%M:%S.%f')
            record['replication_key'] = replication_key

            increment_state(
                state,
                replication_key=self.replication_key,
                latest_record=record,
                is_sorted=self.is_sorted,
                check_sorted=self.check_sorted
            )

            yield record

class PriceStreamWide(BaseStream):
    replication_key = "timestamp"
    is_timestamp_replication_key = True

    def __init__(self, tap: Tap, catalog_entry: dict) -> None:
        super().__init__(tap, catalog_entry)
        self.yf_params = self.stream_params.get('yf_params')
        self.price_tap = YFinancePriceTap(schema_category=self.schema_category)

    @property
    def partitions(self):
        """
          No partitions when running wide stream. Performance will increase because all relevant tickers are batch
          downloaded at once, but data integrity will be likely be sacrificed (see yfinance docs).
        """

        return None

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """
        Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.

        Args:
            context: Stream partition or context dictionary.
        """

        if self._ticker_download_calls == 0:
            self.logger.info(f"Tickers have not been downloaded yet. Downloading now...")
            self.download_tickers(self.stream_params)

        assert isinstance(self.tickers, list), f'self.tickers must be a list, but it is of type {type(self.tickers)}.'

        yf_params = self.yf_params.copy()
        state = self.get_context_state(context)

        if state and 'progress_markers' in state.keys():
            start_date = \
                datetime.fromisoformat(state.get('progress_markers').get('replication_key_value')).strftime('%Y-%m-%d')
        else:
            start_date = self.config.get('default_start_date')

        yf_params['start'] = start_date

        df = self.price_tap.download_price_history_wide(tickers=self.tickers, yf_params=yf_params)
        df.sort_values(by='timestamp', inplace=True)

        for record in df.to_dict(orient='records'):
            record['replication_key'] = record['timestamp']

            increment_state(
                state,
                replication_key=self.replication_key,
                latest_record=record,
                is_sorted=self.is_sorted,
                check_sorted=self.check_sorted
            )

            yield {'data': record, self.replication_key: record['replication_key']}