{{ config(materialized='incremental',unique_key='nome') }}

SELECT DISTINCT
    nome_colonnina AS nome,
    latitudine,
    longitudine,
    nome_quartiere,
    current_timestamp AS insert_time,
    current_timestamp AS update_time
FROM {{ ref('stg_rilevazione_geo') }}

{% if is_incremental() %}
  AND nome_colonnina NOT IN (SELECT nome FROM {{ this }})
{% endif %}