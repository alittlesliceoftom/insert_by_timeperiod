

{% macro run_insert_by_timeperiod_loop(target_schema, target_table, target_relation,unique_key, period,start_stop_dates, backfill, timestamp_field,on_schema_change) %}
"""
This does the actual for loop between start and end dates.
Following params can vary behaviour: 
1. Backfill: If provided then we will:
    a) run the loop for all periods between start and end dates.
    b) Run an upsert operation base on the unique key if present. Else we will delete the whole row and re-insert it.

Note that the start and stop dates are found inisde the period_boundaries dict.
"""

    -- Get period boundaries. This involves querying our source table, so we only do it inside the loop, not in the init statement


    {% set period_boundaries = get_period_boundaries(target_schema, target_table, timestamp_field,
                                                        start_stop_dates.start_date,
                                                        start_stop_dates.stop_date,
                                                        period,
                                                        backfill) %}
                                                        
    {{ dbt_utils.log_info("Period boundaries  = " ~  period_boundaries) }}

    {%- set to_drop = [] -%}

    {%- set loop_vars = {'sum_rows_inserted': 0} -%}

    {{ dbt_utils.log_info("Entering by period loop. Periodic run details: {}".format(period_boundaries)) }}

    {% for i in range(1, period_boundaries.num_periods+1) -%}
        {# The number of periods returned by func is 1 to high for daily model #}
        {# But it's too LOW for the monthly model....  #}
        {# {%- set iteration_number = i +1 %} #}
        {%- set iteration_number = i %}
        {% set periods_to_offset = i - 1 %}

        {%- set period_of_load = get_first_date_of_period_of_load(period, periods_to_offset, period_boundaries.start_timestamp) -%}

        {{ dbt_utils.log_info("Running for {} {} of {} ({}) [{}]".format(period, iteration_number, period_boundaries.num_periods, period_of_load, model.unique_id)) }}

        {%- set tmp_identifier = target_relation.identifier ~ '__dbt_incremental_period' ~ i ~ '_tmp' -%}

        
        {%- if unique_key is not none -%}
            {% set tmp_relation_type = 'table' %}
        {%- else -%}
            {% set tmp_relation_type = 'view' %}
        {%- endif -%}

        {# create the relation that will hold any temp tables. #}            
        {{dbt_utils.log_info("Creating relation: {}".format(tmp_identifier))}}             {# Uncomment line for debug print #}
        {%- set tmp_relation = api.Relation.create(
                                            identifier=tmp_identifier,
                                            database = target_relation.database,
                                            schema = target_relation.schema, 
                                            type=tmp_relation_type) -%}


        {% set tmp_table_sql = get_period_filter_sql(sql, period,
                                                                period_boundaries.start_timestamp,
                                                                period_boundaries.stop_timestamp, periods_to_offset) %}


        {# If unique_key is provided tmp_relation needs to be a table as it will be scanned twice in the delete/insert operation, otherwise it can be a view to minimize intermediate materializations #}
        {%- if unique_key is not none -%}
            {% call statement() -%}
                {{ synapse__create_intermediate_table_as(True, tmp_relation, tmp_table_sql) }} -- Updated to default intermediate tables to HEAP indexes to prevent excess index building.
            {%- endcall %}
        {%- else -%}
            {% call statement() -%}
                {# We need to drop the relations before we create them #}
                {% do adapter.drop_relation(tmp_relation) %}
                {{ synapse__create_view_as(tmp_relation, tmp_table_sql) }}
            {%- endcall %}
        {% endif %}

        {{ adapter.expand_target_column_types(from_relation=tmp_relation,
                                                to_relation=target_relation) }}

                                                
        {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
        {% set target_columns = process_schema_changes(on_schema_change, tmp_relation, target_relation) %}
        {% if not target_columns %}
        {% set target_columns = adapter.get_columns_in_relation(target_relation) %}
        {% endif %}

        {%- set target_cols_csv = target_columns | map(attribute='quoted') | join(', ') -%}
        {%- set insert_query_name = 'main-' ~ i -%}

        {% call statement(insert_query_name, fetch_result=True) -%}
            {%- if backfill -%}
            {{ dbt_utils.log_info("Deleting rows in {} for date {}".format(target_relation, period_of_load)) }}

            DELETE FROM {{ target_relation }}
            WHERE -- avoid between for explicitness
                {{ timestamp_field }} >= {{get_period_filter_from(period, offset = periods_to_offset ,start_timestamp=period_boundaries.start_timestamp)}} AND
                {{ timestamp_field }} < {{get_period_filter_to(period, offset= periods_to_offset ,start_timestamp=period_boundaries.start_timestamp,stop_timestamp=period_boundaries.stop_timestamp)}}
            ;
            {%- elif unique_key is not none -%}
            DELETE
            FROM {{ target_relation }}
            WHERE ({{ unique_key }}) in (
                SELECT ({{ unique_key }})
                FROM {{ tmp_relation }}
            );
            {%- endif %}    
        {{ dbt_utils.log_info("Inserting into {} for date {}".format(target_relation, period_of_load))}}
            INSERT INTO {{ target_relation }} ({{ target_cols_csv }})
            (
                SELECT 
                    {{ target_cols_csv }}
                FROM {{ tmp_relation.include(schema=True) }}
            );
        {%- endcall %}

        {% set result = load_result(insert_query_name) %}

        {% if 'response' in result.keys() %} {# added in v0.19.0 #}
            {% set rows_inserted = result['response']['rows_affected'] %}
        {% else %} {# older versions #}
            {% set rows_inserted = result['status'].split(" ")[2] | int %}
        {% endif %}

        {%- set sum_rows_inserted = loop_vars['sum_rows_inserted'] + rows_inserted -%}
        {%- do loop_vars.update({'sum_rows_inserted': sum_rows_inserted}) %}

        {{ dbt_utils.log_info("Ran for {} {} of {} ({}); {} records inserted [{}]".format(period, iteration_number,
                                                                                            period_boundaries.num_periods,
                                                                                            period_of_load, rows_inserted,
                                                                                            model.unique_id)) }}

        {% do to_drop.append(tmp_relation) %}
        {% do adapter.commit() %}

    {% endfor %}

    {% call noop_statement('main', "INSERT {}".format(loop_vars['sum_rows_inserted']) ) -%}
        {{ tmp_table_sql }}
    {%- endcall %}

    {% for rel in to_drop %}
        {% if rel.type is not none %}
            {% do adapter.drop_relation(rel) %}
        {% endif %}
    {% endfor %}
{% endmacro %}