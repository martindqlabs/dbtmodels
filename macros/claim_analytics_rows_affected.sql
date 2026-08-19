{% macro get_rows_affected_from_result(res) %}
  {# Normalize adapter_response (mapping) and common Snowflake/dbt keys. #}
  {% set ar = none %}
  {% if res is mapping and res.get('adapter_response') %}
    {% set ar = res.get('adapter_response') %}
  {% elif res.adapter_response is defined %}
    {% set ar = res.adapter_response %}
  {% endif %}
  {% if not ar %}
    {{ return(none) }}
  {% endif %}
  {% if ar is mapping %}
    {% set ra = ar.get('rows_affected', ar.get('query_rows_affected', none)) %}
    {% if ra is none and ar.get('response') is mapping %}
      {% set inner = ar.get('response') %}
      {% set ra = inner.get('rows_affected', inner.get('query_rows_affected', none)) %}
    {% endif %}
    {{ return(ra) }}
  {% endif %}
  {{ return(none) }}
{% endmacro %}

{% macro log_rows_affected_console(results) %}
  {% if execute and results and var('rows_affected_console', true) %}
    {{ log("rows_affected (per model; from adapter_response)", info=True) }}
    {% for res in results %}
      {% if res.node is defined and res.node.resource_type == 'model' %}
        {% set ra = get_rows_affected_from_result(res) %}
        {% set label = ra if ra is not none else 'null' %}
        {% set rel = res.relation_name if res.relation_name is defined else (
          res.node.relation_name if res.node.relation_name is defined else res.node.name
        ) %}
        {{ log('  ' ~ rel ~ ' | rows_affected=' ~ label ~ ' | status=' ~ res.status, info=True) }}
      {% elif res is mapping and res.get('unique_id', '').startswith('model.') %}
        {% set ra = get_rows_affected_from_result(res) %}
        {% set label = ra if ra is not none else 'null' %}
        {{ log('  ' ~ res.get('relation_name', res.get('unique_id')) ~ ' | rows_affected=' ~ label ~ ' | status=' ~ res.get('status'), info=True) }}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro ensure_rows_affected_audit_table() %}
  {% if execute %}
    {% set create_schema %}
      create schema if not exists DQLABS_QA.RUNFAILURE
    {% endset %}
    {% do run_query(create_schema) %}
    {% set create_table %}
      create table if not exists DQLABS_QA.RUNFAILURE.DBT_ROWS_AFFECTED_LOG (
        run_at timestamp_tz,
        invocation_id varchar(255),
        resource_type varchar(64),
        node_name varchar(255),
        status varchar(64),
        rows_affected number(38,0),
        execution_time_sec float,
        message varchar(16777216)
      )
    {% endset %}
    {% do run_query(create_table) %}
  {% endif %}
{% endmacro %}

{% macro log_rows_affected_from_run(results) %}
  {% if execute and results %}
    {% for res in results %}
      {% if res.node is defined and res.node.resource_type == 'model' %}
        {% set ra = get_rows_affected_from_result(res) %}
        {% if ra is none %}
          {% set ra_sql = 'null::number' %}
        {% else %}
          {% set ra_sql = (ra | string) ~ '::number' %}
        {% endif %}
        {% set msg = (res.message or '') | replace("'", "''") %}
        {% set nm = res.node.name | replace("'", "''") %}
        {% set st = res.status | string | replace("'", "''") %}
        {% set rt = res.node.resource_type | string | replace("'", "''") %}
        {% set et = res.execution_time | default(0) %}
        {% set sql %}
          insert into DQLABS_QA.RUNFAILURE.DBT_ROWS_AFFECTED_LOG (
            run_at,
            invocation_id,
            resource_type,
            node_name,
            status,
            rows_affected,
            execution_time_sec,
            message
          ) select
            current_timestamp(),
            '{{ invocation_id }}',
            '{{ rt }}',
            '{{ nm }}',
            '{{ st }}',
            {{ ra_sql }},
            {{ et }}::float,
            '{{ msg }}'
        {% endset %}
        {% do run_query(sql) %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro claim_analytics_hooks_run_start() %}
  {% if var('claim_analytics_audit', false) %}
    {{ ensure_rows_affected_audit_table() }}
  {% endif %}
{% endmacro %}

{% macro claim_analytics_hooks_run_end(results) %}
  {{ log_rows_affected_console(results) }}
  {% if var('claim_analytics_audit', false) %}
    {{ log_rows_affected_from_run(results) }}
  {% endif %}
{% endmacro %}
