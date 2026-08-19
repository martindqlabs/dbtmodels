{% materialization custom_incremental, adapter='snowflake' %}

{% set target_relation = this %}
{% set tmp_relation = this.incorporate(path={'identifier': this.identifier ~ '__tmp'}) %}

{# Step 1: Create temp table #}
{% call statement('create_tmp') %}
create or replace table {{ tmp_relation }} as
(
    {{ sql }}
);
{% endcall %}

{% set rows_affected = 0 %}

{# Step 2: Check if target exists #}
{% set existing_relation = adapter.get_relation(
    database=target_relation.database,
    schema=target_relation.schema,
    identifier=target_relation.identifier
) %}

{# Step 3: Create or Merge #}
{% if existing_relation is none %}
    {# First run → create table #}
    {% call statement('create_table') %}
        create table {{ target_relation }} as
        select * from {{ tmp_relation }}
    {% endcall %}

    {# Count rows #}
    {% set count_result = run_query("select count(*) from " ~ tmp_relation) %}
    {% if count_result is not none %}
        {% set rows_affected = count_result.columns[0].values()[0] %}
    {% endif %}

{% else %}
    {# Incremental → Merge #}
    {% call statement('merge') %}
        merge into {{ target_relation }} as target
        using {{ tmp_relation }} as source
        on target.CLAIM_ID = source.CLAIM_ID
           and target.TX_ID = source.TX_ID
        when matched and source.UPDATED_DATE > target.UPDATED_DATE then
            update set
                CLAIM_NUMBER = source.CLAIM_NUMBER,
                POLICY_NUMBER = source.POLICY_NUMBER,
                CLAIM_TYPE = source.CLAIM_TYPE,
                STATUS = source.STATUS,
                STATE = source.STATE,
                CLAIM_DATE = source.CLAIM_DATE,
                ACCIDENT_DATE = source.ACCIDENT_DATE,
                REPORTED_DATE = source.REPORTED_DATE,
                TX_DATE = source.TX_DATE,
                UPDATED_DATE = source.UPDATED_DATE,
                TX_AMOUNT = source.TX_AMOUNT,
                TX_TYPE = source.TX_TYPE,
                TX_CREATED_DATE = source.TX_CREATED_DATE,
                LOAD_TS = source.LOAD_TS
        when not matched then insert (
            CLAIM_ID,
            TX_ID,
            CLAIM_NUMBER,
            POLICY_NUMBER,
            CLAIM_TYPE,
            STATUS,
            STATE,
            CLAIM_DATE,
            ACCIDENT_DATE,
            REPORTED_DATE,
            TX_DATE,
            UPDATED_DATE,
            TX_AMOUNT,
            TX_TYPE,
            TX_CREATED_DATE,
            LOAD_TS
        )
        values (
            source.CLAIM_ID,
            source.TX_ID,
            source.CLAIM_NUMBER,
            source.POLICY_NUMBER,
            source.CLAIM_TYPE,
            source.STATUS,
            source.STATE,
            source.CLAIM_DATE,
            source.ACCIDENT_DATE,
            source.REPORTED_DATE,
            source.TX_DATE,
            source.UPDATED_DATE,
            source.TX_AMOUNT,
            source.TX_TYPE,
            source.TX_CREATED_DATE,
            source.LOAD_TS
        );
    {% endcall %}

    {# Count rows affected by merge #}
    {% set count_result = run_query("select count(*) from " ~ tmp_relation) %}
    {% if count_result is not none %}
        {% set rows_affected = count_result.columns[0].values()[0] %}
    {% endif %}
{% endif %}

{# Step 4: Drop temp table #}
{% call statement('drop_tmp') %}
drop table if exists {{ tmp_relation }}
{% endcall %}

{# Step 5: Return rows affected #}
{% do return({
    "relations": [target_relation],
    "adapter_response": {"rows_affected": rows_affected}
}) %}

{% endmaterialization %}