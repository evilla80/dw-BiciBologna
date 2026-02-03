{{ config(
    materialized='incremental',
    unique_key=['data_idData', 'ora_idOra', 'colonnina_idColonnina'],
    alias='dm_rilevazione'
) }}

SELECT
    -- CHIAVI ESTERNE (Surrogate Keys)
    d.idData as data_idData,
    o.idOra as ora_idOra,
    c.idColonnina as colonnina_idColonnina,
    m.idMeteo as meteo_idMeteo,

    -- MISURE
    r.direzioneCentro,
    r.direzionePeriferia,
    r.totale,

    current_timestamp as load_time

FROM {{ ref('ods_rilevazione') }} r

-- JOIN CON LE NUOVE TABELLE "dm_*"
JOIN {{ ref('dm_data') }} d ON r.data = d.data
JOIN {{ ref('dm_ora') }} o ON r.ora = o.ora
JOIN {{ ref('dm_colonnina') }} c ON r.nome_colonnina = c.nome_colonnina
LEFT JOIN {{ ref('dm_meteo') }} m
ON date_trunc('hour', r.timestamp_completo) = date_trunc('hour', m.timestamp_completo)

{% if is_incremental() %}
    WHERE r.insert_time > (SELECT MAX(load_time) FROM {{ this }})
{% endif %}