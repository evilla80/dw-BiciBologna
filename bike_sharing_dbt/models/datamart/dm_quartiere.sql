/*
    Caricamento incrementale Dimensione Quartiere con generazione chiave surrogata
*/

{{ config(materialized='incremental', unique_key=['idQuartiere'], alias='dm_quartiere') }}

{% set initialize %}
    CREATE SEQUENCE IF NOT EXISTS seq_dm_quartiere;
{% endset %}
{% do run_query(initialize) %}

SELECT
    IFNULL(target.idQuartiere, nextval('seq_dm_quartiere')) as idQuartiere,
    q.nome as nome_quartiere,
    q.superficie,
    q.perimetro,
    q.latitudine_centro,
    q.longitudine_centro

FROM {{ ref('ods_quartiere') }} as q
    LEFT JOIN {{ this }} as target ON q.nome = target.nome_quartiere

{% if is_incremental() %}
    WHERE q.update_time > (
        SELECT COALESCE(MAX(time), '1900-01-01 00:00:00')
        FROM last_execution_times
        WHERE target_table = '{{ this.identifier }}'
    )
{% endif %}