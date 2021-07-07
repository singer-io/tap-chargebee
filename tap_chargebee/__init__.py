import singer
import tap_framework

import tap_chargebee.client
import tap_chargebee.streams

LOGGER = singer.get_logger()


class ChargebeeRunner(tap_framework.Runner):
    pass


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(
        required_config_keys=['api_key', 'start_date', 'site'])

    client = tap_chargebee.client.ChargebeeClient(args.config)

    runner = ChargebeeRunner(
        args, client, get_available_streams(args, client)
    )

    try:
        # Verify start date format
        singer.utils.strptime(args.config.get("start_date"))
    except ValueError:
        raise ValueError("start_date must be in 'YYYY-mm-ddTHH:MM:SSZ' format") from None

    if args.discover:
        runner.do_discover()
    else:
        runner.do_sync()


if __name__ == '__main__':
    main()


def get_available_streams(self, cb_client):
    site_name = self.config.get('site')
    LOGGER.info("Site Name {}".format(site_name))
    configuration_url = 'https://{}.chargebee.com/api/v2/configurations'.format(site_name)
    response = cb_client.make_request(
        url=configuration_url,
        method='GET')
    site_configurations = response['configurations']
    LOGGER.info("Configurations API response {}".format(response))
    product_catalog_version = next(iter(config['product_catalog_version'] for config in site_configurations if
                                        config['domain'].lower() == site_name.lower()),
                                   None)
    if product_catalog_version == 'v2':
        available_streams = tap_chargebee.streams.ITEM_MODEL_AVAILABLE_STREAMS
        self.config['item_model'] = True
        LOGGER.info('Item Model')
    elif product_catalog_version == 'v1':
        available_streams = tap_chargebee.streams.PLAN_MODEL_AVAILABLE_STREAMS
        self.config['item_model'] = False
        LOGGER.info('Plan Model')
    else:
        LOGGER.error("Incorrect Product Catalog version {}".format(product_catalog_version))
        raise RuntimeError("Incorrect Product Catalog version")
    return available_streams
