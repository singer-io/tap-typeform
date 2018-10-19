# TypeForm

This tap is for pulling [Responses](https://developer.typeform.com/responses/) data from the TypeForm API.  Each form responses data set follows a similar pattern: There is header information, then there are responses with answers for those questions.  The form questions are obtained via an additional call packaged into the tap.

## Connecting TypeForm

### TypeForm Setup Requirements

TypeForm uses Oath2 for the Responses, but this tap was developed using a non-expirable token [migrated](https://developer.typeform.com/get-started/convert-keys-to-access-tokens/) from an API key.  You will still need to go through the Oath2 process to get that token, but that is outside the scope of this tap.

### Setup TypeForm as a Stitch source

1. [Sign into your Stitch account](https://app.stitchdata.com/)

2. On the Stitch Dashboard page, click the **Add Integration** button.

3. Click the **TypeForm** icon.

4. Enter a name for the integration. This is the name that will display on the Stitch Dashboard for the integration; it’ll also be used to create the schema in your destination. For example, the name "Stitch TypeForm" would create a schema called `stitch_typeform` in the destination. **Note**: Schema names cannot be changed after you save the integration.

5. In the **Token** field, enter your TypeForm web token.

6. In the **Forms** field, enter a comma delimited list of form ids.  

7. In the **Incremental Range** field, enter the desired incremental frame (daily or hourly).

8. In the **Start Date** field, enter the minimum, beginning start date for data from the form (e.g. 2017-01-1).

---

## TypeForm Replication

With each run of the integration, the following data set is extracted and replicated to the data warehouse:


- **Questions**: A list of question titles and ids that can then be used to link to answers.

- **Landings**: A list of form landings and supporting data since the last completed run of the tap through the most recent day or hour respectively. On the first run, ALL increments since the **Start Date** will be replicated.

- **Answers**: A list of form answers with ids that can be used to link to landings and questions since the last completed run of the integration) through the most recent day or hour respectively. On the first run, ALL increments since the **Start Date** will be replicated.

---

## TypeForm Table Schemas

### questions

- Table name: questions 
- Description: A list of form questions
- Primary key: form_id, question_id
- Replicated fully
- Bookmark column: None
- API endpoint documentation: [Forms](https://developer.typeform.com/create/reference/retrieve-form/)

### landings

- Table name: landings 
- Description: A list of form landings
- Primary key: landing_id
- Replicated incrementally
- Bookmark column: submitted_at (written as resume_date in the state records)
- API endpoint documentation: [Responses](https://developer.typeform.com/responses/)

### answers

- Table name: answers 
- Description: A list of answers with a link back to landings
- Primary key: landing_id, question_id
- Replicated incrementally
- Bookmark column: landing_id
- API endpoint documentation: [Responses](https://developer.typeform.com/responses/)

---

## Troubleshooting / Other Important Info

- **Question Data**: The form definitions are quite robust, but we have chosen to limit the fields to just those needed for responses analysis.

- **Form Data**: The raw response data is not fully normalized and the tap output reflects this by breaking it into landings and answers.  Answers could potentially be normalized further, but the redundant data is quite small so it seemed better to keep it flat.  The hidden field was left a JSON structure since it could have any sorts or numbers of custom elements.  

- **Timestamps**: All timestamp columns are in yyyy-MM-ddTHH:mm:ssZ format.  Resume_date state parameter are Unix timestamps.

