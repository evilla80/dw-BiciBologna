/*
    Tabella transazionale delle rilevazioni
*/

{{ config(materialized='incremental', unique_key=['timestamp_completo', 'nome_colonnina'], alias='rilevazione') }}

SELECT
    timestamp_completo,
    CAST(timestamp_completo AS DATE) AS data,
    EXTRACT(HOUR FROM timestamp_completo) AS ora,
    nome_colonnina,
    direzioneCentro,
    direzionePeriferia,
    totale,
    insert_time,
    update_time
FROM {{ ref('ods_rilevazione_transform') }}

{% if is_incremental() %}
  WHERE update_time > (SELECT IFNULL(MAX(update_time), '0001-01-01 00:00:00') FROM {{ this }})
{% endif %}