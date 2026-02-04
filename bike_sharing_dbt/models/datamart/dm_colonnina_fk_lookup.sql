/*
    Trasformazione Colonnine: Join Spaziale per associare il Quartiere (Dimensione)
    corrispondente e recuperare la chiave surrogata.
*/

{{ config(materialized='ephemeral') }}

SELECT
    c.nome as nome_colonnina,
    c.latitudine,
    c.longitudine,
    c.update_time,

    -- Recuperiamo la chiave surrogata del quartiere (Foreign Key)
    MAX(dq.idQuartiere) as quartiere_idQuartiere

FROM {{ ref('ods_colonnina') }} c
    -- JOIN SPAZIALE: Il punto della colonnina è dentro il poligono del quartiere?
    INNER JOIN {{ ref('ods_quartiere') }} oq
        ON ST_Within(ST_Point(c.longitudine, c.latitudine), oq.geom_perimetro)

    -- JOIN DIMENSIONALE: Recupero ID dal Data Mart Quartieri
    INNER JOIN {{ ref('dm_quartiere') }} dq
        ON oq.nome = dq.nome_quartiere

GROUP BY c.nome, c.latitudine, c.longitudine, c.update_time