# tap-frontapp

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from FrontApp's [API](https://dev.frontapp.com/)
- Extracts the following resources from FrontApp
  - [Analytics](https://dev.emarsys.com/v2/email-campaigns/list-email-campaigns)
      - Hourly/Daily analytics of metrics
          - team_table
- Outputs the schema for each resource

## Configuration

This tap requires a `config.json` which specifies details regarding [API authentication](https://dev.frontapp.com/#authentication), a cutoff date for syncing historical data, and a time period range [daily,hourly] to control what incremental extract date ranges are. See [config.sample.json](config.sample.json) for an example.

To run `tap-frontapp` with the configuration file, use this command:

```bash
› tap-frontapp -c my-config.json
```

---

Copyright &copy; 2018 Stitch
