{{ config(materialized='incremental', unique_key='timestamp_completo', alias='meteo') }}

SELECT *
FROM {{ ref('ods_meteo_transform') }}

{% if is_incremental() %}
  WHERE timestamp_completo > (SELECT IFNULL(MAX(timestamp_completo), '0001-01-01 00:00:00') FROM {{ this }})
{% endif %}