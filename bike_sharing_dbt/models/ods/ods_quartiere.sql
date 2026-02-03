{{ config(materialized='incremental',unique_key='nome' ) }}

SELECT DISTINCT
    nome_quartiere AS nome,
    superficie,
    perimetro,
    latitudine_centro,
    longitudine_centro,
    current_timestamp AS insert_time,
    current_timestamp AS update_time
FROM {{ ref('stg_rilevazione_geo') }}
WHERE nome_quartiere IS NOT NULL

{% if is_incremental() %}
  AND nome_quartiere NOT IN (SELECT nome FROM {{ this }})
{% endif %}