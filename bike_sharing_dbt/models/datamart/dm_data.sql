{{ config(
    materialized='incremental',
    unique_key=['idData'],
    alias='dm_data'
) }}

{% set initialize %}
    CREATE SEQUENCE IF NOT EXISTS seq_dm_data;
{% endset %}
{% do run_query(initialize) %}

SELECT
    {% if is_incremental() %}
        IFNULL(target.idData, nextval('seq_dm_data'))
    {% else %}
        nextval('seq_dm_data')
    {% endif %} as idData,

    source.data,
    EXTRACT(YEAR FROM source.data) as anno,
    EXTRACT(MONTH FROM source.data) as mese,
    EXTRACT(DAY FROM source.data) as giorno,

    CASE
        WHEN EXTRACT(ISODOW FROM source.data) IN (6, 7) THEN 1
        ELSE 0
    END as is_weekend,

    current_timestamp as insert_time,
    current_timestamp as update_time

FROM (SELECT DISTINCT data FROM {{ ref('ods_rilevazione') }}) as source

{% if is_incremental() %}
    LEFT JOIN {{ this }} as target ON source.data = target.data
    WHERE target.idData IS NULL
{% endif %}