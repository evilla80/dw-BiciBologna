{{ config(materialized='incremental',unique_key='timestamp_completo') }}

SELECT
    time AS timestamp_completo,
    temperature_2m AS temperatura,
    apparent_temperature AS temperatura_percepita,
    weather_code AS codice_meteo,
    CASE
        WHEN weather_code = 0 THEN 'Cielo Sereno'
        WHEN weather_code BETWEEN 1 AND 3 THEN 'Nuvoloso'
        WHEN weather_code BETWEEN 45 AND 48 THEN 'Nebbia'
        WHEN weather_code BETWEEN 51 AND 67 THEN 'Pioggia'
        WHEN weather_code BETWEEN 71 AND 77 THEN 'Neve'
        WHEN weather_code BETWEEN 80 AND 82 THEN 'Rovesci'
        WHEN weather_code >= 95 THEN 'Temporale'
        ELSE 'Altro'
    END AS descrizione,

    current_timestamp AS insert_time,
    current_timestamp AS update_time
FROM {{ source('staging', 'raw_meteo') }}

{% if is_incremental() %}
  WHERE time > (SELECT MAX(timestamp_completo) FROM {{ this }})
{% endif %}