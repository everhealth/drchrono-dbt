Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

--------

## Notes from Alex
Here's what I've done so far:
- made a dbt user on the Staging cluster (username: `dbt_stg`)
- created `.sql` files for each model that can be built on the cluster by `dbt_stg` user from CLI
- Added subdirectories to `/models` for DaySheet and AR
- - `/models/daysheet/cash` has definitions for each of the 4 MV's required to build `daysheet_patientpayments_merged`
- added a macro `/macros/daysheet_pp_columns.sql` to help with generating the SQL for each MV
- Updated all the table references with `{{ref()}}` / `{{source()}}` so the DAGs will build
- Generated docs and hosted them as a [Github page](https://alexwickstrom.github.io/dbt_docs/#!/overview)

TODO:

- [x] Add credits&adjustments MVs
- [x] Add `daysheet_patientpayments_merged` view definition
- [x] Use the `{{ref()}}` macro inside the VIEW definitions to reference the MV's
- [x] Add a unit test for PatientPayments MV's
- [x] Add a unit test for Debits MV's (recency)
- [x] Add a unit test for Credits MV's (recency)
- [x] Add a unit tests for A/R MV's (not_null, recency)
- [ ] Set up hosting for dbt project
