# Changelog
## 1.1.0
  *  Adds support for Item Model, Multi-decimal (for Plan Model), and Account hierarchy (for Plan Model) [#56](https://github.com/singer-io/tap-chargebee/pull/56)
  * Organized the folder structure: 
      a. common(common schemas to both plan model and item model) 
      b. item_model 
      c. plan_model
  * Introduces two new streams: ITEM_MODEL_AVAILABLE_STREAMS, PLAN_MODEL_AVAILABLE_STREAMS

## 1.0.3
  * Fix invalid JSON from #44

## 1.0.2
  * Remove `maxLength` from `payment_sources` schema to address certain integrations having IDs of greater length than specified, and make the schema more flexible as the API evolves [#44](https://github.com/singer-io/tap-chargebee/pull/44)

## 1.0.0
  * No change from 0.0.12

## 0.0.12
  * Add `custom_fields` to plans, addons, customers, and subscriptions [#9](https://github.com/singer-io/tap-chargebee/pull/9)

## 0.0.3
  * Add `credit_notes` stream [#2](https://github.com/singer-io/tap-chargebee/pull/2)
