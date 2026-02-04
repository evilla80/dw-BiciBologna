/*
    Materializzazione incrementale dati meteo
*/

{{ config(materialized='incremental', unique_key='timestamp_completo', alias='meteo') }}

SELECT *
FROM {{ ref('ods_meteo_transform') }}

{% if is_incremental() %}
  WHERE update_time > (SELECT IFNULL(MAX(update_time), '0001-01-01 00:00:00') FROM {{ this }})
{% endif %}