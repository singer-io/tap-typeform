# Changelog

## 2.1.0
  * Fixes/Handles following [#73](https://github.com/singer-io/tap-typeform/pull/73)
    * Syncs data for all forms if `forms` field is missing in config and makes the field as optional
    * Prevents tap from failing when there are no questions associated with a form

## 2.0.1
  * Adds a condition to check if `submitted_landings` stream's child key is blank [#69](https://github.com/singer-io/tap-typeform/pull/69)

## 2.0.0
  * Splits `landings` stream to `submitted_landings` and `unsubmitted_landings` streams with new replication keys [#64](https://github.com/singer-io/tap-typeform/pull/64)
  * Adds field `sub_questions` for the `questions` group [#62](https://github.com/singer-io/tap-typeform/pull/62)
  * Adds missing fields for `forms` and `questions` streams [#63](https://github.com/singer-io/tap-typeform/pull/63)
  * Bookmark changes for parent and child streams and code refactoring [#64](https://github.com/singer-io/tap-typeform/pull/64)
  * Modifies integration tests for code changes [#65](https://github.com/singer-io/tap-typeform/pull/65)

## 1.4.5
  * Fix `landings` stream to be incremental [#58](https://github.com/singer-io/tap-typeform/pull/58)

## 1.4.4
  * Request TimeOut Implementation [#55](https://github.com/singer-io/tap-typeform/pull/55)

## 1.4.3
  * Adds retry to `ChunkedEncodingError` on requests [#52](https://github.com/singer-io/tap-typeform/pull/52)

## 1.4.2
  * Fixes IndexError in `landings` pagination [#51](https://github.com/singer-io/tap-typeform/pull/51)

## 1.4.1
  * Adds pagination to the `landings` stream [#48](https://github.com/singer-io/tap-typeform/pull/48)

## 1.4.0
  * Update the tap to make less requests and ask for bigger batches of data per response [#44](https://github.com/singer-io/tap-typeform/pull/44)
  * Fix bookmarking bug. Bookmarks will never be set in the future now [#43](https://github.com/singer-io/tap-typeform/pull/43)

## 1.3.2
  * This version actually reverts the code back to `v1.2.0` [#39](https://github.com/singer-io/tap-typeform/pull/39)

## 1.3.1
  * Revert back to `v1.2.0`

## 1.3.0
  * Add the Forms stream [#29](https://github.com/singer-io/tap-typeform/pull/29)
  * Add integration tests [#32](https://github.com/singer-io/tap-typeform/pull/32)
  * Add unit test for exception handling [#33](https://github.com/singer-io/tap-typeform/pull/33)
  * Add config validation to ensure only valid forms are input by the user [#34](https://github.com/singer-io/tap-typeform/pull/34)
  * Fix a bug in the bookmarks test [#36](https://github.com/singer-io/tap-typeform/pull/36)

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
