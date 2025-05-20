import singer
import json
import os
import pytz
from singer import metadata, metrics
from singer import Transformer
from singer.catalog import Catalog
from tap_chargebee.client import ChargebeeClient
from datetime import datetime


LOGGER = singer.get_logger()
UNIX_SECONDS_INTEGER_DATETIME_PARSING = "unix-seconds-integer-datetime-parsing"
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
# 2 minutes lookback window to avoid missing records
LOOKBACK_WINDOW = 2


class BaseChargebeeStream:

    STREAM = None
    KEY_PROPERTIES = []
    API_METHOD = "GET"
    REQUIRES = []
    REPLICATION_METHOD = None
    REPLICATION_KEY = None
    ENTITY = None
    SELECTED_BY_DEFAULT = True
    VALID_REPLICATION_KEYS = []
    INCLUSION = None
    SCHEMA = None
    SORT_BY = None

    def __init__(
        self, config: dict, state: dict, catalog: Catalog, client: ChargebeeClient
    ):
        self.config = config
        self.state = state
        self.catalog = catalog
        self.client = client
        self.include_deleted = self.get_include_deleted()

    def get_abs_path(self, path: str):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

    def handle_deleted_items(self, records: list):
        return []

    def get_include_deleted(self):
        """
        Returns whether to include deleted records based on config.
        """
        return self.config.get("include_deleted") not in ["false", "False", False]

    def add_custom_fields(self, record: dict):
        """
        Placeholder for adding custom fields. Should be overridden by subclasses if needed.
        """
        return record

    def load_shared_schema_refs(self):
        """
        Loads shared schema references from common and version-specific folders.
        Returns a dictionary of shared schemas.
        """
        shared_schema_refs = {}
        schema_folders = ["common"]

        if self.config["item_model"]:
            # Chosen streams of product catalog v2
            schema_folders.append("item_model")
        else:
            # Chosen streams of product catalog v1
            schema_folders.append("plan_model")

        for schema_folder in schema_folders:
            shared_schema_refs.update(self.load_shared_schema_ref(schema_folder))

        return shared_schema_refs

    def load_shared_schema_ref(self, folder_name: str):
        """
        Loads schema references from a specified folder.
        Returns a dictionary of schema references.
        """
        shared_schema_refs = {}
        shared_schemas_path = self.get_abs_path("../schemas/" + folder_name)

        try:
            shared_file_names = [
                f
                for f in os.listdir(shared_schemas_path)
                if os.path.isfile(os.path.join(shared_schemas_path, f))
            ]
        except FileNotFoundError as e:
            LOGGER.error("Schema folder not found: %s", shared_schemas_path)
            return {}

        for shared_file in shared_file_names:
            # Excluded event stream as it is not used as a reference in any other stream
            if shared_file == "events.json":
                continue
            with open(os.path.join(shared_schemas_path, shared_file)) as data_file:
                shared_schema_refs[shared_file] = json.load(data_file)

        return shared_schema_refs

    def load_schema(self):
        """
        Loads the schema for the current stream.
        """
        schema_file = "../schemas/{}.json".format(self.SCHEMA)
        with open(self.get_abs_path(schema_file), encoding="UTF-8") as f:
            schema = json.load(f)

        return schema

    def generate_catalog(self):
        """
        Generates the catalog for the stream, including schema and metadata.
        """
        schema = self.load_schema()
        mdata = metadata.new()

        # Metadata to add to the catalog
        metadata_to_add = {
            "forced-replication-method": self.REPLICATION_METHOD,
            "valid-replication-keys": self.VALID_REPLICATION_KEYS,
            "inclusion": self.INCLUSION,
            "table-key-properties": self.KEY_PROPERTIES,
        }

        # Assign metadata at the stream level
        for k, v in metadata_to_add.items():
            mdata = metadata.write(mdata, (), k, v)

        # Assign metadata for each property in the schema
        for field_name, field_schema in schema.get("properties").items():
            inclusion = "available"

            # Set inclusion to automatic for key properties and replication keys
            if (
                field_name in self.KEY_PROPERTIES
                or field_name in self.VALID_REPLICATION_KEYS
            ):
                inclusion = "automatic"

            mdata = metadata.write(
                mdata, ("properties", field_name), "inclusion", inclusion
            )

        # Resolve shared schema references
        refs = self.load_shared_schema_refs()

        return [
            {
                "tap_stream_id": self.STREAM,
                "stream": self.STREAM,
                "schema": singer.resolve_schema_references(schema, refs),
                "metadata": metadata.to_list(mdata),
            }
        ]

    def appendCustomFields(self, record: dict):
        """
        Prepare custom fields for the record for objects like "addon", "plan", "subscription", "customer" from the /events endpoint
        """
        listOfCustomFieldObj = ["addon", "plan", "subscription", "customer"]
        custom_fields = {}
        event_custom_fields = {}

        if self.ENTITY == "event":
            # Extracting the object name from the event_type and adding custom fields for the object
            words = record["event_type"].split("_")
            content_obj = "_".join(words[:-1])
            content_data = record["content"].get(content_obj, {})

            # Add custom fields for specific objects
            if content_obj in listOfCustomFieldObj:
                for k, v in content_data.items():
                    if "cf_" in k:
                        event_custom_fields[k] = v
                record["content"][content_obj]["custom_fields"] = json.dumps(
                    event_custom_fields
                )

        for k, v in record.items():
            if "cf_" in k:
                custom_fields[k] = v
        record["custom_fields"] = json.dumps(custom_fields)
        return record

    def update_bookmark(self, bookmark_value: str):
        """
        Updates the bookmark in the state if the new bookmark value is greater than the current one.
        """
        start_date = self.config.get("start_date")
        current_bookmark = singer.get_bookmark(
            self.state, self.STREAM, self.REPLICATION_KEY, default=start_date
        )
        if bookmark_value and bookmark_value > current_bookmark:
            self.state = singer.write_bookmark(
                self.state, self.STREAM, self.REPLICATION_KEY, bookmark_value
            )

    def evaluate_bookmark_based_on_lookback(
        self, last_bookmark_value: str, lookback_window: int
    ):
        """
        Adjusts the bookmark value based on the lookback window.
        """
        bookmark_value_timestamp = int(
            singer.utils.strptime_to_utc(last_bookmark_value).timestamp()
        )
        lookback_evaluated_bookmark = bookmark_value_timestamp - lookback_window
        return lookback_evaluated_bookmark

    def sync(self):
        """
        Extract data from the Chargebee API and write it to the singer stream.
        """
        start_date = self.config.get("start_date")
        last_bookmark_value = singer.get_bookmark(
            self.state, self.STREAM, self.REPLICATION_KEY, default=start_date
        )

        # Adjust the bookmark value based on the lookback window
        # Ref: https://github.com/singer-io/tap-chargebee/pull/54#issuecomment-1138439726
        lookback_evaluated_bookmark = self.evaluate_bookmark_based_on_lookback(
            last_bookmark_value, LOOKBACK_WINDOW
        )

        # Filtering parameters
        params = {f"{self.REPLICATION_KEY}[after]": lookback_evaluated_bookmark}

        LOGGER.info(f"Querying stream - {self.STREAM} starting at {self.STREAM}")

        pages = self.client.get_offset_based_pages(
            self.get_url(), self.API_METHOD, self.SORT_BY, params
        )

        with metrics.record_counter(self.STREAM) as counter, Transformer(
            integer_datetime_fmt=UNIX_SECONDS_INTEGER_DATETIME_PARSING
        ) as transformer:
            for page in pages:
                # Extract deleted records of "plans, addons and coupons" streams from events
                deleted_records = self.handle_deleted_items(page)
                # Combine current page records with deleted records
                records_to_write = page + deleted_records

                for record in records_to_write:
                    record = self.add_custom_fields(record[self.ENTITY])

                    replication_key_value = singer.utils.strftime(
                        datetime.fromtimestamp(
                            record.get(self.REPLICATION_KEY), pytz.UTC
                        ),
                        format_str=DATETIME_FORMAT,
                    )

                    transformed_record = transformer.transform(
                        record,
                        self.catalog.schema.to_dict(),
                        metadata.to_map(self.catalog.metadata),
                    )
                    singer.write_record(self.STREAM, transformed_record)
                    self.update_bookmark(replication_key_value)
                    counter.increment()

                # Write state after each page
                singer.write_state(self.state)
            return counter.value
