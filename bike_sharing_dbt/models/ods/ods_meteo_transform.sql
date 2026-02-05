/*
    Standardizzazione tecnica (nomi e tipi), nessuna logica di business.
*/
{{ config(materialized='ephemeral') }}

SELECT
    time AS timestamp_completo,
    CAST(temperature_2m AS DECIMAL(10,2)) AS temperatura,
    CAST(apparent_temperature AS DECIMAL(10,2)) AS temperatura_percepita,
    CAST(weather_code AS INTEGER) AS weather_code, -- Lasciamo il codice grezzo!

    current_timestamp AS insert_time,
    current_timestamp AS update_time

FROM {{ source('staging', 'raw_meteo') }}