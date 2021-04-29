# Changelog

## 1.0.2
  * Remove `maxLength` from `payment_sources` schema to address certain integrations having IDs of greater length than specified, and make the schema more flexible as the API evolves [#44](https://github.com/singer-io/tap-chargebee/pull/44)

## 1.0.0
  * No change from 0.0.12

## 0.0.12
  * Add `custom_fields` to plans, addons, customers, and subscriptions [#9](https://github.com/singer-io/tap-chargebee/pull/9)

## 0.0.3
  * Add `credit_notes` stream [#2](https://github.com/singer-io/tap-chargebee/pull/2)
