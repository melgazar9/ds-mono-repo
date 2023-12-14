"""YFinance tap class."""

from __future__ import annotations

import singer_sdk.typing as th
from singer_sdk import Tap
from singer_sdk._singerlib.catalog import CatalogEntry, MetadataMapping, Schema, Metadata
from tap_yfinance.streams import TickerStream, PriceStream, PriceStreamWide
from tap_yfinance.schema import get_schema


class TapYFinance(Tap):
    """YFinance tap class."""

    name = "tap-yfinance"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "financial_category",
            th.ObjectType(
                additional_properties=th.ObjectType()
            ),
            required=True
        ),
    ).to_dict()

    @staticmethod
    def discover_catalog_entry(financial_category: str, table_name: str, schema: dict) -> CatalogEntry:
        """Create `CatalogEntry` object for the given collection."""

        # TODO: Why is it if I set both tap_stream_id and stream to be financial_category + '|' + table_name it deselects ALL streams?

        required_fields = Schema.from_dict(schema).to_dict().get('required')

        required_fields = [] if required_fields is None else required_fields
        key_fields = list(Schema.from_dict(schema).to_dict().get('properties').keys())

        category_entry = CatalogEntry(
                tap_stream_id=table_name,
                stream=table_name,
                table=table_name,
                key_properties=required_fields,
                schema=Schema.from_dict(schema),
                replication_method=None,  # defined by user
                metadata=MetadataMapping.get_standard_metadata(
                    schema=schema,
                    schema_name=financial_category,  # TODO: Fix this! This is just a placeholder.
                    replication_method=None,  # defined by user
                    key_properties=key_fields,
                    valid_replication_keys=None  # defined by user
                ),
                replication_key=None  # defined by user
            )

        return category_entry

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """

        if hasattr(self, "_catalog_dict") and self._catalog_dict:  # pylint: disable=access-member-before-definition
            return self._catalog_dict  # pylint: disable=access-member-before-definition

        if self.input_catalog:
            return self.input_catalog.to_dict()

        financial_categories: dict[str, dict[str, dict]] = self.config.get('financial_category', {})

        result: dict[str, list[dict]] = {"streams": []}

        for fc in financial_categories.keys():
            for table_name in financial_categories[fc].keys():
                schema = get_schema(financial_categories[fc][table_name]['schema_category'])
                catalog_entry: CatalogEntry = \
                    self.discover_catalog_entry(financial_category=fc, table_name=table_name, schema=schema)

                result["streams"].append(catalog_entry.to_dict())

        self._catalog_dict: dict = result

        return self._catalog_dict

    def discover_streams(self) -> list:
        """
        Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """

        streams = []
        for catalog_entry in self.catalog_dict["streams"]:
            financial_category = catalog_entry['metadata'][-1]['metadata']['schema-name']
            if financial_category == 'prices' and not catalog_entry['tap_stream_id'].endswith('wide'):
                stream = PriceStream(self, catalog_entry=catalog_entry)
            elif financial_category == 'prices' and catalog_entry['tap_stream_id'].endswith('wide'):
                stream = PriceStreamWide(self, catalog_entry=catalog_entry)
            elif financial_category == 'tickers':
                stream = TickerStream(self, catalog_entry=catalog_entry)
            else:
                raise ValueError('Could not set the proper stream.')
            streams.append(stream)

        return streams


if __name__ == "__main__":
    TapYFinance.cli()