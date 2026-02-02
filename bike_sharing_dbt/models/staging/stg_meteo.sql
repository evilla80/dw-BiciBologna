{{ config(materialized='view') }}

SELECT
    -- 1. Chiave Temporale
    date_trunc('hour', CAST(time AS TIMESTAMP)) as orario_meteo,

    -- 2. Dati Numerici
    temperature_2m as temperatura,
    apparent_temperature as temperatura_percepita,
    weather_code as codice_meteo_originale,

    CASE
        WHEN weather_code IN (0, 1) THEN 'Soleggiato'
        WHEN weather_code IN (2, 3, 45, 48) THEN 'Nuvoloso/Nebbia'
        WHEN weather_code IN (51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82) THEN 'Pioggia'
        WHEN weather_code IN (71, 73, 75, 77, 85, 86) THEN 'Neve'
        WHEN weather_code >= 95 THEN 'Temporale'
        ELSE 'Altro'
    END as condizione_macro,

    CASE
        WHEN temperature_2m < 0 THEN 'Gelo (<0°C)'
        WHEN temperature_2m BETWEEN 0 AND 10 THEN 'Freddo (0-10°C)'
        WHEN temperature_2m BETWEEN 10 AND 20 THEN 'Mite (10-20°C)'
        WHEN temperature_2m BETWEEN 20 AND 30 THEN 'Caldo (20-30°C)'
        ELSE 'Torrido (>30°C)'
    END as fascia_temperatura

FROM {{ source('staging', 'raw_meteo') }}