# Changelog

## 1.2.0
  * Add logic to handle empty forms

## 1.1.4
  * Revert `v1.1.3`, effectively making `v1.1.4` the same as `v1.1.2`

## 1.1.3
  * Fix bookmarking to use the max `submitted_at` value instead of the end
    of our query's date window [#16](https://github.com/singer-io/tap-typeform/pull/16)

## 1.1.2
  * Use `dict.get()` to access more fields rather than `[]` [#3](https://github.com/singer-io/tap-typeform/pull/5)

## 1.1.1
  * Use `dict.get()` to access fields rather than `[]` [#3](https://github.com/singer-io/tap-typeform/pull/3)

## 1.1.0
  * Add table/field selection [#2](https://github.com/singer-io/tap-typeform/pull/2)

## 1.0.3
  * Fix `landings` stream formatting

## 1.0.2
  * Add type (string) to `landing_id` in `landings` stream

## 1.0.1
  * Pinned `backoff` version to `1.3.2`

## 1.0.0
  * General release of the tap
