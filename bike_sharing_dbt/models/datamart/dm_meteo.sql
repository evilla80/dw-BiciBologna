{{ config(materialized='incremental', unique_key=['id_meteo'], alias='dm_meteo') }}

{% set initialize %}
    CREATE SEQUENCE IF NOT EXISTS seq_dm_meteo;
{% endset %}
{% do run_query(initialize) %}

SELECT
    {% if is_incremental() %}
        IFNULL(target.id_meteo, nextval('seq_dm_meteo'))
    {% else %}
        nextval('seq_dm_meteo')
    {% endif %} as id_meteo,
    m.timestamp_completo,
    m.temperatura,
    m.temperatura_percepita,
    m.codice_meteo,
    CASE
        WHEN m.codice_meteo = 0 THEN 'Cielo Sereno'
        WHEN m.codice_meteo BETWEEN 1 AND 3 THEN 'Nuvoloso'
        WHEN m.codice_meteo BETWEEN 45 AND 48 THEN 'Nebbia'
        WHEN m.codice_meteo BETWEEN 51 AND 67 THEN 'Pioggia'
        WHEN m.codice_meteo BETWEEN 71 AND 77 THEN 'Neve'
        WHEN m.codice_meteo BETWEEN 80 AND 82 THEN 'Rovesci'
        WHEN m.codice_meteo >= 95 THEN 'Temporale'
        ELSE 'Altro'
    END AS descrizione,
    CASE
        WHEN m.temperatura < 10 THEN 'Freddo'
        WHEN m.temperatura BETWEEN 10 AND 25 THEN 'Mite'
        ELSE 'Caldo'
    END as fascia_temperatura

FROM {{ ref('ods_meteo') }} as m

{% if is_incremental() %}
    LEFT JOIN {{ this }} target ON m.timestamp_completo = target.timestamp_completo
{% endif %}


{% if is_incremental() %}
    WHERE m.update_time > (
        SELECT COALESCE(MAX(time), '1900-01-01 00:00:00')
        FROM last_execution_times
        WHERE target_table = '{{ this.identifier }}'
    )
{% endif %}