{{ config(materialized='incremental', unique_key=['idMeteo'], alias='dm_meteo') }}

{% set initialize %}
    CREATE SEQUENCE IF NOT EXISTS seq_dm_meteo;
{% endset %}
{% do run_query(initialize) %}

SELECT
    IFNULL(target.idMeteo, nextval('seq_dm_meteo')) as idMeteo,

    m.timestamp_completo,
    m.temperatura,
    m.temperatura_percepita,
    m.weather_code as codice_meteo,

    CASE
        WHEN m.weather_code = 0 THEN 'Cielo Sereno'
        WHEN m.weather_code BETWEEN 1 AND 3 THEN 'Nuvoloso'
        WHEN m.weather_code BETWEEN 45 AND 48 THEN 'Nebbia'
        WHEN m.weather_code BETWEEN 51 AND 67 THEN 'Pioggia'
        WHEN m.weather_code BETWEEN 71 AND 77 THEN 'Neve'
        WHEN m.weather_code BETWEEN 80 AND 82 THEN 'Rovesci'
        WHEN m.weather_code >= 95 THEN 'Temporale'
        ELSE 'Altro'
    END AS descrizione,

    CASE
        WHEN m.temperatura < 10 THEN 'Freddo'
        WHEN m.temperatura BETWEEN 10 AND 25 THEN 'Mite'
        ELSE 'Caldo'
    END as fascia_temperatura,


FROM {{ ref('ods_meteo') }} as m
    LEFT JOIN {{ this }} as target ON m.timestamp_completo = target.timestamp_completo

{% if is_incremental() %}
    WHERE m.update_time > (
        SELECT COALESCE(MAX(time), '1900-01-01 00:00:00')
        FROM last_execution_times
        WHERE target_table = '{{ this.identifier }}'
    )
{% endif %}