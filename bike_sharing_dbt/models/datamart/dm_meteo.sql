{{ config(
    materialized='incremental',
    unique_key=['idMeteo'],
    alias='dm_meteo'
) }}

{% set initialize %}
    CREATE SEQUENCE IF NOT EXISTS seq_dm_meteo;
{% endset %}
{% do run_query(initialize) %}

SELECT
    {% if is_incremental() %}
        IFNULL(target.idMeteo, nextval('seq_dm_meteo'))
    {% else %}
        nextval('seq_dm_meteo')
    {% endif %} as idMeteo,

    source.timestamp_completo,
    source.temperatura,
    source.temperatura_percepita,
    source.descrizione,

    CASE
        WHEN source.temperatura < 10 THEN 'Freddo'
        WHEN source.temperatura BETWEEN 10 AND 25 THEN 'Mite'
        ELSE 'Caldo'
    END as fascia_temperatura,

    current_timestamp as insert_time,
    current_timestamp as update_time

FROM {{ ref('ods_meteo') }} as source

{% if is_incremental() %}
    LEFT JOIN {{ this }} as target
    ON source.timestamp_completo = target.timestamp_completo
    WHERE source.insert_time > (SELECT MAX(update_time) FROM {{ this }})
{% endif %}