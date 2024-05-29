
{% macro insert_by_timeperiod_initialisation(target_relation, existing_relation, period, start_stop_dates, full_refresh_mode, sample_select_for_table_schema, tmp_relation) %}
    {{ dbt_utils.log_info("Running initialisation for insert by timeperiod") }}
    

    {%- set to_drop = [] -%}
    {% if existing_relation is none %}
        {{ dbt_utils.log_info("Sample select for table schema is: " ~ sample_select_for_table_schema ) }}
        {% if sample_select_for_table_schema is not none %}
            {% set build_sql = create_table_as(False, target_relation, sample_select_for_table_schema) %}
        {% else %}
            {% set filtered_sql = replace_placeholder_with_period_filter(sql,
                                                                        start_stop_dates.start_date,
                                                                        start_stop_dates.stop_date,
                                                                        0, period) %}
            {% set build_sql = create_table_as(False, target_relation, filtered_sql) %}

            {% do to_drop.append(tmp_relation) %}
        {% endif %}
    
    {% elif existing_relation.is_view or full_refresh_mode %}
        {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
        {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
        {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}

        {% do adapter.drop_relation(backup_relation) %}
        {% do adapter.rename_relation(target_relation, backup_relation) %}

        {% if sample_select_for_table_schema is not none %}
            {% set build_sql = create_table_as(False, target_relation, sample_select_for_table_schema) %}
        {% else %}

            {% set filtered_sql = replace_placeholder_with_period_filter(sql,
                                                                        start_stop_dates.start_date,
                                                                        start_stop_dates.stop_date,
                                                                        0, period) %}
            {% set build_sql = create_table_as(False, target_relation, filtered_sql) %}

        {% endif %}

        {% do to_drop.append(tmp_relation) %}
        {% do to_drop.append(backup_relation) %}

    {% endif %}


    {# Commit the init build #}
    {% if build_sql is defined %}
        
        {{ dbt_utils.log_info("Starting Init table build") }}

        {% call statement("main", fetch_result=True) %}
            {{ build_sql }}
        {% endcall %}

        {% set result = load_result('main') %}

        {% if 'response' in result.keys() %} {# added in v0.19.0 #}
            {% set rows_inserted = result['response']['rows_affected'] %}
        {% else %} {# older versions #}
            {% set rows_inserted = result['status'].split(" ")[2] | int %}
        {% endif %}

        {% call noop_statement('main', "BASE LOAD {}".format(rows_inserted)) -%}
            {{ build_sql }}
        {%- endcall %}

        {% do adapter.commit() %}

        {{ run_hooks(post_hooks, inside_transaction=True) }}

        {{ dbt_utils.log_info("Initial table built for {} starting {} model: [{}]".format(period, start_stop_dates.start_date, model.unique_id)) }}

    {% else %}
        {{ dbt_utils.log_info("No initial build required") }}
    {% endif %}



    {# Tidy up from init build - drop the temp relations.  #}

    {% for rel in to_drop %}
        {% if rel.type is not none %}
            {% do adapter.drop_relation(rel) %}
        {% endif %}
    {% endfor %}

{% endmacro %}