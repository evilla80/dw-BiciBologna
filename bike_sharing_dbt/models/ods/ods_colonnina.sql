{{ config(materialized='incremental', unique_key='nome', alias='colonnina') }}

SELECT *
FROM {{ ref('ods_colonnina_transform') }}

{% if is_incremental() %}
  WHERE timestamp_completo > (SELECT IFNULL(MAX(timestamp_completo), '0001-01-01 00:00:00') FROM {{ this }})
{% endif %}