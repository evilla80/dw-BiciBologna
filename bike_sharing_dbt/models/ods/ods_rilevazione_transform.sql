{{ config(materialized='ephemeral') }}

SELECT
    CAST(data AS TIMESTAMP) AS timestamp_completo,
    colonnina AS nome_colonnina,
    direzione_centro AS direzioneCentro,
    direzione_periferia AS direzionePeriferia,
    totale,
    current_timestamp AS insert_time,
    current_timestamp AS update_time
FROM {{ source('staging', 'raw_colonnine') }}