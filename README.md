# tap-typeform

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This tap:

- Pulls raw data from TypeForms's [API](https://api.typeform.com/forms)
- Extracts the following resources from TypeForm
  - [Responses](https://developer.typeform.com/responses)
      - List of questions on each form added to the configuration in the tap.
      - List of landings of users onto each form added to the configuration in the tap.
      - List of answers completed during each landing onto each form added to the configuration in the tap.
- Outputs the schema for each resource

## Setup

Building follows the conventional Singer setup:

python3 ./setup.py clean
python3 ./setup.py build
python3 ./setup.py install

## Configuration

This tap requires a `config.json` which specifies details regarding an [Authentication token](https://developer.typeform.com/get-started/convert-keys-to-access-tokens/), a list of form ids, a start date for syncing historical data (date format of YYYY-MM-DDTHH:MI:SSZ), request_timeout for which request should wait to get response(It is an optional parameter and default request_timeout is 300 seconds). See [example.config.json](example.config.json) for an example.

Create the catalog:

```bash
› tap-typeform --config config.json --discover > catalog.json
```

Then to run the extract:

```bash
› tap-typeform --config config.json --catalog catalog.json --state state.json
```

Note that a typical state file looks like this:

```json
{"bookmarks": {"team_table": {"date_to_resume": "2018-08-01 00:00:00"}}}
```

## Replication

With each run of the integration, the following data set is extracted and replicated to the data warehouse:

- **Questions**: A list of question titles and ids that can then be used to link to answers.

- **Landings**: A list of form landings and supporting data since the last completed run of the tap through the most recent day or hour respectively. On the first run, ALL increments since the **Start Date** will be replicated.

- **Answers**: A list of form answers with ids that can be used to link to landings and questions since the last completed run of the integration) through the most recent day or hour respectively. On the first run, ALL increments since the **Start Date** will be replicated.

## Troubleshooting / Other Important Info

- **Question Data**: The form definitions are quite robust, but we have chosen to limit the fields to just those needed for responses analysis.

- **Form Data**: The raw response data is not fully normalized and the tap output reflects this by breaking it into landings and answers.  Answers could potentially be normalized further, but the redundant data is quite small so it seemed better to keep it flat.  The hidden field was left a JSON structure since it could have any sorts or numbers of custom elements.  

- **Timestamps**: All timestamp columns are in yyyy-MM-ddTHH:mm:ssZ format.  Resume_date state parameter are Unix timestamps.

---

Copyright &copy; 2018 Stitch
