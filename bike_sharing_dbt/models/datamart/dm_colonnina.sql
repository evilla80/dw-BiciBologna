/*
    Caricamento Dimensione Colonnina con Sequence
*/

{{ config(materialized='incremental', unique_key=['id_colonnina'], alias='dm_colonnina') }}

{% set initialize %}
    CREATE SEQUENCE IF NOT EXISTS seq_dm_colonnina;
{% endset %}
{% do run_query(initialize) %}

SELECT
    {% if is_incremental() %}
        IFNULL(target.id_colonnina, nextval('seq_dm_colonnina'))
    {% else %}
        nextval('seq_dm_colonnina')
    {% endif %} as id_colonnina,
    c.nome_colonnina,
    c.latitudine,
    c.longitudine,
    c.quartiere_idQuartiere

FROM {{ ref('dm_colonnina_fk_lookup') }} as c
{% if is_incremental() %}
    LEFT JOIN {{ this }} as target ON c.nome_colonnina = target.nome_colonnina
{% endif %}

{% if is_incremental() %}
    WHERE c.update_time > (
        SELECT COALESCE(MAX(time), '1900-01-01 00:00:00')
        FROM last_execution_times
        WHERE target_table = '{{ this.identifier }}'
    )
{% endif %}