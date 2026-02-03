{{config(materialized='incremental',unique_key=['timestamp_completo', 'nome_colonnina']) }}

SELECT
    timestamp_completo,
    -- Tronchiamo la data per averla pulita (YYYY-MM-DD)
    CAST(timestamp_completo AS DATE) AS data,
    EXTRACT(HOUR FROM timestamp_completo) AS ora,

    nome_colonnina,
    direzioneCentro,
    direzionePeriferia,
    totale,

    extraction_time AS insert_time,
    extraction_time AS update_time
FROM {{ ref('stg_rilevazione_geo') }}

{% if is_incremental() %}
  WHERE timestamp_completo > (SELECT MAX(timestamp_completo) FROM {{ this }})
{% endif %}