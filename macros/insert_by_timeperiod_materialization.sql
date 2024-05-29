
{% materialization insert_by_timeperiod, adapter = 'sqlserver' -%}
    {# {{ dbt_utils.log_info("Insert by time period called:") }} -- uncomment for debugging  #}

    {%- set full_refresh_mode = flags.FULL_REFRESH -%}
    {%- set backfill = var("backfill")|lower|trim == 'true' -%}

    {% if backfill %}
        {{ dbt_utils.log_info("WARNING: Backfill run detected, please note this run will overwrite data. If you're not familiar with this, please pause the run.") }}
        {% if backfill and full_refresh_mode %}
        {{ dbt_utils.log_info("Setting full_refresh_mode for model" ~ this ~ "to false as backfill is set to true") }}  -- This enables us to FULL REFRESH incremental models and backfill IBTP models at the same time
        {%- set full_refresh_mode = false -%}
        {% endif %} 
    {% endif %}

    {%- set target_relation = this -%}
    {%  set target_schema = schema %}
    {%  set target_table = target_relation.name %}
    {%- set existing_relation = load_relation(this) -%}
    {%- set tmp_relation = make_temp_relation(this) -%}

    {%- set target_relation = api.Relation.create(
        database = target_relation.database,
        schema = target_relation.schema,
        identifier = target_relation.identifier,
        type = 'table'
    ) -%}

    {%- set timestamp_field = config.require('timestamp_field') -%}
    {%- set date_source_models = config.get('date_source_models', default=none) -%}
    {%- set unique_key = config.get('unique_key', default=none) -%}
    {%- set sample_select_for_table_schema = config.get('sample_select_for_table_schema', default=none) -%}
    
    {# We can take start and stop dates from 3 sources. Either config (default), another table (rarely used), or from a CLI argument. We want the CLI argument to over-ride the other options.  #}
    {%  set start_date_cli = var("start_date", default=none ) %}
    {%  set stop_date_cli = var("stop_date", default=none) %}
    
    {%  if backfill  %}  -- catch incomplete backfill arguments. 
        {{ dbt_utils.log_info("Backfill run detected")}}
        {% if not start_date_cli  or not  stop_date_cli %}
            {{ exceptions.raise_compiler_error("Backfill requires start date and stop date as CLI arguments") }}
        {% endif %}
    {% endif %}

    {%  if start_date_cli is not none  and stop_date_cli is not none %}
        {%  set start_date_cli = start_date_cli|lower|trim %}
        {%  set stop_date_cli = stop_date_cli|lower|trim %}
        {{ dbt_utils.log_info("Using start and stop dates from CLI: " ~ start_date_cli ~ ' and ' ~ stop_date_cli) }}
        {%- set start_stop_dates = {'start_date': start_date_cli, 'stop_date': stop_date_cli} | as_native -%}
    {% else %}
        {%- set start_stop_dates = get_start_stop_dates(timestamp_field, date_source_models) | as_native -%}
    {% endif %}

    {%- set period = config.get('period', default='day') -%}


    {# Uncomment below line for debugging help #}
    {{ dbt_utils.log_info("start_stop_dates are: " ~ start_stop_dates.start_date ~ ' and ' ~ start_stop_dates.stop_date) }} 



    {%- do check_period_filter_placeholders(sql) -%}

    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    {# Run init if necessary: #}
    {% if full_refresh_mode or existing_relation.is_view or existing_relation is none %}

        {{ dbt_utils.log_info("About to initialise the table, this will include deleting the existing table" ~ this  ~ "If you don't want this, please exit now. Note this can happen when there is no existing table, in this case, existing relation = " ~ existing_relation ) }}

        {{ insert_by_timeperiod_initialisation(target_relation, existing_relation, period, start_stop_dates, full_refresh_mode, sample_select_for_table_schema, tmp_relation) }}
        {% set on_schema_change = 'ignore' %}
    {% else %}
        {# We only want this to not be ignore if we didn't full refresh #}
        {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
    {% endif %}
    
    {# Loop - we always enter the loop#}
    {{ run_insert_by_timeperiod_loop(target_schema, target_table, target_relation,unique_key, period, start_stop_dates, backfill, timestamp_field, on_schema_change) }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
