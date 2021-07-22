# tap-chargebee

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [Chargebee API](https://apidocs.chargebee.com/docs/api)
- Extracts the following resources:
  - [Addons](https://apidocs.chargebee.com/docs/api/addons)
  - [Coupons](https://apidocs.chargebee.com/docs/api/coupons)
  - [Credit Notes](https://apidocs.chargebee.com/docs/api/credit_notes)
  - [Comments](https://apidocs.chargebee.com/docs/api/comments)  
  - [Customers](https://apidocs.chargebee.com/docs/api/customers)
  - [Events](https://apidocs.chargebee.com/docs/api/events)
  - [Gifts](https://apidocs.chargebee.com/docs/api/gifts)
  - [Invoices](https://apidocs.chargebee.com/docs/api/invoices)
  - [Orders](https://apidocs.chargebee.com/docs/api/orders)
  - [Payment Sources](https://apidocs.chargebee.com/docs/api/payment_sources)
  - [Plans](https://apidocs.chargebee.com/docs/api/plans)
  - [Subscriptions](https://apidocs.chargebee.com/docs/api/subscriptions)
  - [Transactions](https://apidocs.chargebee.com/docs/api/transactions)
  - [Virtual Bank Accounts](https://apidocs.chargebee.com/docs/api/virtual_bank_accounts)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick Start

1. Install

    ```bash
    pip install tap-chargebee
    ```

2. Create the config file

   Create a JSON file called `config.json`. Its contents should look like:

   ```json
    {
        "start_date": "2010-01-01T00:00:00Z",
        "api_key": "<Chargebee API Key>",
        "site": "<Chargebee Site>",
        "include_deleted": "True|False"
    }
    ```

   The `start_date` specifies the date in ISO(YYYY-mm-ddTHH:MM:SSZ) format at which the tap will begin pulling data
   (for those resources that support this).

   The `api_key` is the API key for your Chargebee site.

   The `site` parameter represents the name of your specific Chargebee site (e.g. `https://{site}.chargebee.com/api/v2/subscriptions`)

   The 'include_deleted' is an optional flag to ask if you want deleted records of all streams or not. Default: true 

4. Run the Tap in Discovery Mode

    ```bash
    tap-chargebee --config config.json --discover > catalog.json
    ```

   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode

    ```bash
    tap-chargebee --config config.json --catalog catalog.json
    ```

---

Copyright &copy; 2019 Stitch
