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

    if args.discover:
        runner.do_discover()
    else:
        runner.do_sync()


if __name__ == '__main__':
    main()


def get_available_streams(self, cb_client):
    configuration_url = 'https://{}.chargebee.com/api/v2/configurations'.format(self.config.get('site'))
    response = cb_client.make_request(
        url=configuration_url,
        method='GET')
    site_configurations = response['configurations']
    product_catalog_version = [config['product_catalog_version'] for config in site_configurations if
                               config['domain'] == self.config.get('site')][0]
    if product_catalog_version == 'v2':
        available_streams = tap_chargebee.streams.ITEM_MODEL_AVAILABLE_STREAMS
        self.config['item_model'] = True
        LOGGER.info('Item Model')
    else:
        available_streams = tap_chargebee.streams.PLAN_MODEL_AVAILABLE_STREAMS
        self.config['item_model'] = False
        LOGGER.info('Plan Model')
    return available_streams
