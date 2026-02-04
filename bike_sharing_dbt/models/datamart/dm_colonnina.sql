/*
    Caricamento Dimensione Colonnina con Sequence
*/

{{ config(materialized='incremental', unique_key=['idColonnina'], alias='dm_colonnina') }}

{% set initialize %}
    CREATE SEQUENCE IF NOT EXISTS seq_dm_colonnina;
{% endset %}
{% do run_query(initialize) %}

SELECT
    IFNULL(target.idColonnina, nextval('seq_dm_colonnina')) as idColonnina,
    c.nome_colonnina,
    c.latitudine,
    c.longitudine,
    c.quartiere_idQuartiere -- FK popolata dal lookup

FROM {{ ref('dm_colonnina_fk_lookup') }} as c
    LEFT JOIN {{ this }} as target ON c.nome_colonnina = target.nome_colonnina

{% if is_incremental() %}
    -- Controlla la data salvata nella tabella last_execution_times per QUESTO modello
    WHERE c.update_time > (
        SELECT COALESCE(MAX(time), '1900-01-01 00:00:00')
        FROM last_execution_times
        WHERE target_table = '{{ this.identifier }}'
    )
{% endif %}