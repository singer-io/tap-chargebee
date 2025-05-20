import singer
import sys
import json
from singer import metadata, Catalog
from tap_chargebee.client import ChargebeeClient
import tap_chargebee.streams as streams

LOGGER = singer.get_logger()


def stream_is_selected(mdata):
    """
    Check if the stream is selected
    """
    return mdata.get((), {}).get("selected", False)


def get_available_streams(config: dict, cb_client: ChargebeeClient):
    """
    Prepare the available streams based on the product catalog version
    """
    site_name = config.get("site")
    LOGGER.info("Site Name %s", site_name)
    configuration_url = f"https://{site_name}.chargebee.com/api/v2/configurations"

    try:
        # Make a request to the configurations API
        response = cb_client.make_request(url=configuration_url, method="GET")
        site_configurations = response["configurations"]
    except Exception as e:
        LOGGER.error("Failed to fetch configurations: %s", e)
        raise e

    # Fetch the product catalog version
    product_catalog_version = next(
        iter(
            config["product_catalog_version"]
            for config in site_configurations
            if config["domain"].lower() == site_name.lower()
        ),
        None,
    )

    if product_catalog_version == "v2":
        available_streams = streams.ITEM_MODEL_AVAILABLE_STREAMS
        config["item_model"] = True
        LOGGER.info("Found product catalog version v2")
    elif product_catalog_version == "v1":
        available_streams = streams.PLAN_MODEL_AVAILABLE_STREAMS
        config["item_model"] = False
        LOGGER.info("Found product catalog version v1")
    else:
        LOGGER.error(
            "Incorrect Product Catalog version {}".format(product_catalog_version)
        )
        raise RuntimeError("Incorrect Product Catalog version")

    return available_streams


def do_discover(config: dict, state: dict, available_streams: list):
    """
    Generate the catalog
    """
    LOGGER.info("Starting discovery.")
    catalog = []

    # Generate catalog for each stream based on the product catalog version
    for available_stream in available_streams:
        stream = available_stream(config, state, None, None)
        catalog += stream.generate_catalog()

    json.dump({"streams": catalog}, sys.stdout, indent=4)
    LOGGER.info("Finished discover mode")


def do_sync(config: dict, catalog: Catalog, state: dict, client: ChargebeeClient):
    """
    Sync data from Chargebee.
    """

    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("last/currently syncing stream: %s", last_stream)

    # Resume sync from the last stream
    for catalog_entry in catalog.get_selected_streams(state):
        mdata = metadata.to_map(catalog_entry.metadata)
        replication_key = metadata.get(mdata, (), "replication-key")
        key_properties = metadata.get(mdata, (), "table-key-properties")
        stream_name = catalog_entry.tap_stream_id

        singer.set_currently_syncing(state, stream_name)
        singer.write_state(state)
        singer.write_schema(
            stream_name, catalog_entry.schema.to_dict(), key_properties, replication_key
        )

        LOGGER.info("%s: Starting sync", stream_name)
        instance = streams.STREAMS[stream_name](config, state, catalog_entry, client)
        counter_value = instance.sync()
        singer.set_currently_syncing(state, None)
        singer.write_state(state)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(
        required_config_keys=["api_key", "start_date", "site"]
    )

    client = ChargebeeClient(args.config)
    available_streams = get_available_streams(args.config, client)

    if args.discover:
        do_discover(args.config, args.state, available_streams)
    elif args.catalog:
        do_sync(args.config, args.catalog, args.state, client)


if __name__ == "__main__":
    main()
