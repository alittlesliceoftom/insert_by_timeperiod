{% macro get_start_stop_dates(timestamp_field, date_source_models) %}

    {% if config.get('start_date', default=none) is not none %}

        {%- set start_date = config.get('start_date') -%}
        {%- set stop_date = config.get('stop_date', default=none) -%}

        {% do return({'start_date': start_date,'stop_date': stop_date}) %}

    {% elif date_source_models is not none %}

        {% if date_source_models is string %}
            {% set date_source_models = [date_source_models] %}
        {% endif %}
        {% set query_sql %}
            WITH stage AS (
            {% for source_model in date_source_models %}
                SELECT 
                    MIN({{ timestamp_field }}) AS MinValue,
                    MAX({{ timestamp_field }}) AS MaxValue 
                FROM {{ ref(source_model) }}
                {% if not loop.last %} UNION ALL {% endif %}
            {% endfor %})

            SELECT MIN(MinValue) AS MIN, MAX(MaxValue) AS MAX
            FROM stage
        {% endset %}

        {% set min_max_dict = dbt_utils.get_query_results_as_dict(query_sql) %}

        {% set start_date = min_max_dict['MIN'][0] | string %}
        {% set stop_date = min_max_dict['MAX'][0] | string %}
        {% set min_max_dates = {"start_date": start_date, "stop_date": stop_date} %}

        {% do return(min_max_dates) %}

    {% else %}
        {%- if execute -%}
            {{ exceptions.raise_compiler_error("Invalid 'insert_by_period' configuration. Must provide 'start_date' and optionally 'stop_date'") }}
        {%- endif -%}
    {% endif %}

{% endmacro %}

{% macro synapse__drop_relation_script(relation) -%}
  {# Delete any associated views #}
  IF object_id ('{{ relation.include(database=False) }}','V') IS NOT NULL
    BEGIN
    DROP VIEW {{ relation.include(database=False) }}
    END
{# Drop associated tables  #}
  ELSE IF object_id ('{{ relation.include(database=False) }}','U') IS NOT NULL
    BEGIN
    DROP TABLE {{ relation.include(database=False) }}
    END
  ;
{% endmacro %}

{% macro check_period_filter_placeholders(model_sql) %}

    {%- if model_sql.find('__PERIOD_FILTER_FROM__') == -1 and model_sql.find('__PERIOD_FILTER_TO__') == -1 -%}
        {%- set error_message -%}
            Model '{{ model.unique_id }}' does not include the required period filters: '__PERIOD_FILTER_FROM__' and/or '__PERIOD_FILTER_TO__' in its sql
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}

{% endmacro %}

{%- macro get_period_filter_from(period, offset, start_timestamp) %}
    {% if period is not defined or offset is not defined or start_timestamp is not defined %}
    {{ exceptions.raise_compiler_error("Missing variable, expected are: period " ~ period ~ " offset " ~ offset ~ " start_timestamp " ~ start_timestamp) }}
    {% endif %}

    """ Generic macro to get the period filter to date. Note this provides text that can be used in a SQL statement, rather than a computed date."""
    {%- set period_filter_from -%}
        DATEADD({{period}}, {{offset}}, CAST('{{ start_timestamp }}' AS DATE))
    {%- endset -%}
    {% do return(period_filter_from) %}
{% endmacro %}

{%- macro get_period_filter_to(period, offset,start_timestamp, stop_timestamp)%}
""" Generic macro to get the period filter to date. Note this provides text that can be used in a SQL statement, rather than a computed date."""
    {%- set period_filter_to -%}
        LEAST(DATEADD({{period}}, {{offset}} + 1, CAST('{{ start_timestamp }}' AS DATE)), '{{ stop_timestamp if stop_timestamp is not none else "9999-12-31" }}')
    {%- endset -%}
    {% do return(period_filter_to) %}
{% endmacro %}

{%- macro replace_placeholder_with_period_filter(core_sql, start_timestamp, stop_timestamp, offset, period) -%}
    -- Get filter values
    {%- set period_filter_from = get_period_filter_from(period, offset,start_timestamp) -%}
    {%- set period_filter_to = get_period_filter_to(period, offset,start_timestamp, stop_timestamp) -%}
    {# Below log lines left if needed for debug #}
    {# {{ dbt_utils.log_info("Period filter from: " ~ period_filter_from) }}
    {{ dbt_utils.log_info("Period filter to: " ~ period_filter_to) }} #}
    -- Replace placeholders with filter values
    {%- set filtered_sql = core_sql | replace("__PERIOD_FILTER_FROM__", period_filter_from) | replace("__PERIOD_FILTER_TO__", period_filter_to) -%}
    {% do return(filtered_sql) %}
{%- endmacro %}

{% macro get_period_boundaries(target_schema, target_table, timestamp_field, start_date, stop_date, period, backfill) -%}
"""Returns start, stop and number of periods for a given target table."""
    {# Uncomment this line for debug. #}
    {# {{ dbt_utils.log_info("Getting period boundaries for target table: " ~ target_schema ~ "." ~ target_table ~ "dates:" ~ start_date ~ ":" stop_date) }}} #}
    {% set period_boundary_sql -%}
        WITH data AS (
            SELECT
            {% if backfill %}
                '{{ start_date }}' AS start_timestamp, -- in backfill mode we want to start with this date
                COALESCE({{ "nullif('" ~ stop_date | lower ~ "','none')" }} ,
                         CURRENT_TIMESTAMP ) as stop_timestamp
            {% else%}
                GREATEST(
                    DATEADD(day, 1, max({{ timestamp_field }})), -- we need to start with the day after the latest date so that we don't recreate the first day. 
                    '{{ start_date }}'
                    ) AS start_timestamp,
                COALESCE({{ "nullif('" ~ stop_date | lower ~ "','none')" }},
                         CURRENT_TIMESTAMP ) as stop_timestamp
            FROM {{ target_schema }}.{{ target_table }}
            {% endif %}
            
        )
        SELECT
            start_timestamp, 
            stop_timestamp,
            {{ datediff('start_timestamp','stop_timestamp',period) }}  + 1   AS num_periods
        FROM data
    {%- endset %}

    {% set period_boundaries_dict = dbt_utils.get_query_results_as_dict(period_boundary_sql) %}

    {% set period_boundaries = {'start_timestamp': period_boundaries_dict['start_timestamp'][0] | string,
                                'stop_timestamp': period_boundaries_dict['stop_timestamp'][0] | string,
                                'num_periods': period_boundaries_dict['num_periods'][0] | int} %}

    {% do return(period_boundaries) %}
{%- endmacro %}



{%- macro get_first_date_of_period_of_load(period, offset, start_timestamp) -%}

    {% set period_of_load_sql -%}
        SELECT DATEADD({{ period }}, {{ offset }}, CAST('{{start_timestamp}}' AS DATE)) AS period_of_load
    {%- endset %}

    {% set period_of_load_dict = dbt_utils.get_query_results_as_dict(period_of_load_sql) %}

    {% set period_of_load = period_of_load_dict['period_of_load'][0] | string %}

    {% do return(period_of_load) %}
{%- endmacro -%}

{%- macro get_period_filter_sql(base_sql, period, start_timestamp, stop_timestamp, offset) -%}
    {%- set filtered_sql = {'sql': base_sql} -%}

    {%- do filtered_sql.update({'sql': replace_placeholder_with_period_filter(filtered_sql.sql,
                                                                                       start_timestamp,
                                                                                       stop_timestamp,
                                                                                       offset, period)}) -%}

    {{  filtered_sql.sql }}

{%- endmacro %}
