{{ config(materialized='ephemeral') }}

SELECT
    c.nome as nome_colonnina,
    c.latitudine,
    c.longitudine,
    c.insert_time,

    -- Recuperiamo la chiave dal nuovo file dm_quartiere
    q.idQuartiere as quartiere_idQuartiere

FROM {{ ref('ods_colonnina') }} c
INNER JOIN {{ ref('dm_quartiere') }} q
ON c.nome_quartiere = q.nome_quartiere