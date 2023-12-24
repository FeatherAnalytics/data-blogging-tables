# data-blogging-tables
## What This Is
A repo to try out and show off [dbt](https://github.com/dbt-labs/dbt-core). This project is configured to run on [BigQuery](https://github.com/dbt-labs/dbt-bigquery). For more dbt-centric resources, I recommend looking at the [awesome-dbt](https://github.com/Hiflylabs/awesome-dbt) repository.

## Project Structure
### Staging
#### [GSheets](https://github.com/FeatherAnalytics/data-blogging-tables/tree/main/models/staging/gsheets)
Data stored on Google Sheets to be ingested.

### Marts
#### [Jinja_tricks](https://github.com/FeatherAnalytics/data-blogging-tables/tree/main/models/marts/jinja_tricks)
Models written to display how to use Jinja. 
##### [Hierarchy_model](https://github.com/FeatherAnalytics/data-blogging-tables/blob/main/models/marts/jinja_tricks/hierarchy_model.sql)
This model shows how loops can be used to generate a hierarchy of an organization. Using `ID` and `MANAGER_ID` along 
with `START_DATE` and `END_DATE`, this model constructs a table which ascends through the organization from the lowest level to the top. The output goes from left to right, with the top of the hierarchy in the right-most columns. This allows us to modify the raw data, adding or removing layers, and dbt easily constructs the model for downstream consumption.

### Compiled
#### [Jinja_tricks](https://github.com/FeatherAnalytics/data-blogging-tables/tree/main/models/compiled/jinja_tricks)
The compiled SQL code for models stored under `marts/jinja_tricks`. Hopefully this allows viewers to see the dbt model and the compiled SQL side-by-side to gain an understanding of what the underlying code is doing.