/*
    Caricamento incrementale Dimensione Quartiere con generazione chiave surrogata
*/

{{ config(materialized='incremental', unique_key=['id_quartiere'], alias='dm_quartiere') }}

{% set initialize %}
    CREATE SEQUENCE IF NOT EXISTS seq_dm_quartiere;
{% endset %}
{% do run_query(initialize) %}

SELECT
    {% if is_incremental() %}
        IFNULL(target.id_quartiere, nextval('seq_dm_quartiere'))
    {% else %}
        nextval('seq_dm_quartiere')
    {% endif %} as id_quartiere,
    q.nome as nome_quartiere,
    q.superficie,
    q.geom_perimetro,
    q.latitudine_centro,
    q.longitudine_centro

FROM {{ ref('ods_quartiere') }} as q
{% if is_incremental() %}
    LEFT JOIN {{ this }} as target ON q.nome = target.nome_quartiere
{% endif %}

{% if is_incremental() %}
    WHERE q.update_time > (
        SELECT COALESCE(MAX(time), '1900-01-01 00:00:00')
        FROM last_execution_times
        WHERE target_table = '{{ this.identifier }}'
        )
{% endif %}