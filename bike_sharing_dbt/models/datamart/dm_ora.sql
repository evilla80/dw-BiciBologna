{{ config(
    materialized='incremental',
    unique_key=['idOra'],
    alias='dm_ora'
) }}

{% set initialize %}
    CREATE SEQUENCE IF NOT EXISTS seq_dm_ora;
{% endset %}
{% do run_query(initialize) %}

SELECT
    {% if is_incremental() %}
        IFNULL(target.idOra, nextval('seq_dm_ora'))
    {% else %}
        nextval('seq_dm_ora')
    {% endif %} as idOra,

    source.ora,

    CASE
        WHEN source.ora BETWEEN 0 AND 5 THEN 'Notte'
        WHEN source.ora BETWEEN 6 AND 11 THEN 'Mattina'
        WHEN source.ora BETWEEN 12 AND 17 THEN 'Pomeriggio'
        ELSE 'Sera'
    END as fascia_oraria,

    current_timestamp as insert_time,
    current_timestamp as update_time

FROM (SELECT DISTINCT ora FROM {{ ref('ods_rilevazione') }}) as source

{% if is_incremental() %}
    LEFT JOIN {{ this }} as target ON source.ora = target.ora
    WHERE target.idOra IS NULL
{% endif %}