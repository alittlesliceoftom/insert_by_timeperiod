# Custom insert by time period materialization (Synapse)

**NOTE:  We have retained slightly different naming (IBTP vs IBP) and folder location from the `insert_by_period` materialisation. Switching from IBP to IBTP is a breaking change for models, IBTP only currently works on Microsoft Synapse and IBTP had a separate developement path from latest IBP materialisation.**

`insert_by_timeperiod` (IBTP) allows dbt to insert records into a table one period (i.e. day, week) at a time.

This materialisation is supported only for synapse. For other adapters, utilise the `insert_by_period` macros.

This materialization is appropriate for event data that can be processed in discrete periods. It is similar in concept to the built-in incremental materialization, but has the added benefit of building the model in chunks even during a full-refresh so is particularly useful for models where the initial run can be problematic.

Should a run of a model using this materialization be interrupted, a subsequent run will continue building the target table from where it was interrupted (granted the `--full-refresh` flag is omitted).

Progress is logged in the command line for easy monitoring.

The synapse `insert_by_timeperiod` materialisation includes a couple of differences from the other implementations:

1. CLI supported backfill mode with insertable start and end dates.

   - Enables you to backfill a range of dates
   - Backfill is done by DELETING whole date, then inserting replacement records (this is performant and simple)
   - Make backfill take priority over full refresh, this is a key change as it means you can run a combined backfill (IBP model) and full refresh (incremental models) at the same time. This enables you to run complex graphs of IBTP and incremental models, backfilling a date range, and they just work.
   - Backfill deletion is done by the timestamp_field
2. Customisable FROM and TO dates inside the materialisation loop enable you to include WINDOW functions in your code.

   - This requires sligthly more effort to use than the `insert_by_period` approach, but is more explicit and configurable, please see usage section below.

## Installation

This is not a package on the Package Hub. To install it via git, add this to `packages.yml`:

```yaml
packages:
  - git: https://github.com/dbt-labs/dbt-labs-experimental-features
    subdirectory: insert_by_timeperiod
    revision: XXXX #optional but highly recommended. Provide a full git sha hash, e.g. 7180db61d26836b931aa6ef8ad9d70e7fb3a69fa. If not provided, uses the current HEAD.
```

### Supported Versions: 

As of 29/05/2024 - only tested on Azure Synapse.

Dbt versions tested: 1.4, 1.7

## Usage:

```sql
{{
  config(
    materialized = "insert_by_timeperiod",
    period = "day",
    timestamp_field = "created_at",
    start_date = "2018-01-01",
    stop_date = "2018-06-01")
}}
with events as (
  select *
  from {{ ref('events') }}
  WHERE 
    created_at>= __PERIOD_FILTER_FROM__ -- This will be replaced with a filter in the materialization code 
    AND
    created_at < __PERIOD_FILTER_TO__  -- This will be replaced with a filter in the materialization code

)
....complex aggregates here....
```

#### Insert By TimePeriod - Solution for Synapse

Config parameters:

| Parameter          | Mandatory?                 | Data type                 | Description                                                                                                                                                                                                                                                                                                                     |
| ------------------ | -------------------------- | ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| start_date         | Mandatory                  | YYYY-MM-DD formatted date | Start date for periodic insert macro, first\_\_PERIOD_FILTER_FROM__ value will be this value, unless destination table has values greater. Recommend to use the macro get_run_from_date(start_date='20yy-mm-dd') here as it can automatically reduce the date range in dev.))                                                  |
| stop_date          | Optional (Default = today) | YYYY-MM-DD formatted date | Last date for periodic insert macro. If not specified will default to current date + 1 day. If specified the last period filled can potentially be after this date if insert period overflows this date. When using period = 'day', this will be an exclusive stop_date, e.g. the preceding date will be the last date written. |
| period             | Optional (default = day)   | day, week, month, or year | Specifies length of the period to be inserted and difference between\_\_PERIOD_FILTER_FROM__ to \_\_PERIOD_FILTER_TO__                                                                                                                                                                                                          |
| timestamp_field    | Mandatory                  | String                    | Name of the date/timestamp column in the resulting table. Starting period to be inserted will be calculated based on the last value in the table                                                                                                                                                                                |
| date_source_models | Optional                   | Array of strings          | List of source models to check date period from. Before running incremental periods the script will query the source models for the range of dates available and will work within MIN and MAX dates found.                                                                                                                      |

### Date placeholders required in the model query

insert_by_period materialization expects query to contain "\_\_PERIOD_FILTER_FROM__" and "\_\_PERIOD_FILTER_TO__" strings.

During model execution they will be dynamically replaced by dates, that represent the time period that needs to be filled for the destination table.
**\_\_PERIOD_FILTER_FROM__ is inclusive, meaning you're expected to write SnashotDate >= \_\_PERIOD_FILTER_FROM__**
**While \_\_PERIOD_FILTER_TO__ is exclusive, meaning that resulting dataset should not contain this date and you're expected to write SnapshotDate < \_\_PERIOD_FILTER_TO__**

Replacement values will have DATE data type, so you could do further date calculations on top of them.

The expectation is that resulting query should only produce rows representative for this date period ("timestamp_field" values should fall only within the period between these dates.)

You can use these placeholders as many times as necessary in the query.

#### Performance

It is recommneded to use this macro **without** a unique key. Using a unique key will drive a delete operations by the unique key that are slow. Instead leave no unique key and test for uniqueness only.

Ideally pick your period of materialisation that for the majority of your data you will be loading in chunks of data that are between 6 and 60 million rows of data at a time.

Where possible use a medium or large RC to improve index quality.

### Commands

#### Run model from last populated date to stop date.

```
dbt run -s modelname
```

#### Backfill For a Specific Date Range

Backfill of date ranges can be done via CLI arguments. This is an advanced feature and should be used wiht care.

Example command that would backfill `base_ibp_model` + it's immediate downstream dependencies for given date range (right date excluded.)

`dbt run -s base_ibp_model+ --vars "{start_date: '2023-09-08', stop_date: '2023-09-10', backfill: true}" --full-refresh`

The `--full-refresh` is overrided by backfilll parameter for IBTP models, but applies for incremental models. This lets you run dependent models together.

E.g. if we imagine this graph (here for demonstration purposes only!):
`base_ibp_model` >> `prep_incremental_model` >> `fact_ibp_model `

The above command would:

1. Backfill `base_ibp_model` for dates `2023-09-08` - `2023-09-10`
2. Full Refresh `prep_incremental_model`
3. Backfill `fact_ibp_model` for dates `2023-09-08` - `2023-09-10`

#### Run model from start date to stop date **deletes prior data at start of run** use with care.

```
dbt run -s modelname --full-refresh
```
