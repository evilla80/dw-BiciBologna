{{ config(materialized='ephemeral') }}

SELECT
    d.id_data as data_idData,
    m.id_meteo as meteo_idMeteo,
    c.id_colonnina as colonnina_idColonnina,
    o.id_ora as ora_idOra,

    r.direzioneCentro,
    r.direzionePeriferia,
    r.totale,

FROM {{ ref('ods_rilevazione') }} r
    INNER JOIN {{ ref('dm_data') }} d ON r.data = d.data
    INNER JOIN {{ ref('dm_ora') }} o ON r.ora = o.ora
    INNER JOIN {{ ref('dm_colonnina') }} c ON r.nome_colonnina = c.nome_colonnina
    LEFT JOIN {{ ref('dm_meteo') }} m
        ON r.timestamp_completo = m.timestamp_completo