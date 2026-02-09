/*
    Lookup delle chiavi surrogate per la Fact Table Rilevazioni.
    Collega i fatti (ODS) alle Dimensioni (DM).
*/

{{ config(materialized='ephemeral') }}

SELECT
    r.timestamp_completo,
    r.nome_colonnina,
    d.id_data as data_idData,
    o.id_ora as ora_idOra,
    c.id_colonnina as colonnina_idColonnina,
    m.id_meteo as meteo_idMeteo,

    -- Misure
    r.direzioneCentro,
    r.direzionePeriferia,
    r.totale,

FROM {{ ref('ods_rilevazione') }} r
    INNER JOIN {{ ref('dm_data') }} d ON r.data = d.data
    INNER JOIN {{ ref('dm_ora') }} o ON r.ora = o.ora
    INNER JOIN {{ ref('dm_colonnina') }} c ON r.nome_colonnina = c.nome_colonnina
    LEFT JOIN {{ ref('dm_meteo') }} m
        ON date_trunc('hour', r.timestamp_completo) = date_trunc('hour', m.timestamp_completo)