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
        required_config_keys=['api_key', 'start_date'])

    full_site = args.config.get('full_site', None)
    site = args.config.get('site', None)
    if full_site is None and site is None:
        raise Exception("Config is missing required key: atleast one key 'site' or 'full_site' is required")

    if full_site is None:
        args.config['full_site'] = f"{site}.chargebee.com"

    client = tap_chargebee.client.ChargebeeClient(args.config)

    runner = ChargebeeRunner(
        args, client, tap_chargebee.streams.AVAILABLE_STREAMS
        )

    if args.discover:
        runner.do_discover()
    else:
        runner.do_sync()


if __name__ == '__main__':
    main()
