/*
    Materializzazione anagrafica Quartieri
*/

{{ config(materialized='incremental', unique_key='nome', alias='quartiere') }}

SELECT
    nome,
    geom_perimetro,
    latitudine_centro,
    longitudine_centro,
    superficie,
    insert_time,
    update_time
FROM {{ ref('ods_quartiere_transform') }}

{% if is_incremental() %}
  WHERE update_time > (SELECT IFNULL(MAX(update_time), '0001-01-01 00:00:00') FROM {{ this }})
{% endif %}