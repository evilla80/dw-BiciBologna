/*
    Materializzazione anagrafica Colonnine
*/

{{ config(materialized='incremental', unique_key='nome', alias='colonnina') }}

SELECT *
FROM {{ ref('ods_colonnina_transform') }}

{% if is_incremental() %}
  WHERE update_time > (SELECT IFNULL(MAX(update_time), '0001-01-01 00:00:00') FROM {{ this }})
{% endif %}