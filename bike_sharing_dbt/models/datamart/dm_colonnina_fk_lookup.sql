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
    -- Recuperiamo la chiave surrogata del quartiere
    MAX(dq.id_quartiere) as quartiere_idQuartiere

FROM {{ ref('ods_colonnina') }} c
    -- faccio left join per avere la colonnina anche se non ho il quartiere
    -- JOIN SPAZIALE: Il punto della colonnina è dentro il poligono del quartiere?
    LEFT JOIN {{ ref('ods_quartiere') }} oq
        ON ST_Within(ST_Point(c.longitudine, c.latitudine), oq.geom_perimetro)

    -- JOIN DIMENSIONALE: Recupero ID dal Data Mart Quartieri
    LEFT JOIN {{ ref('dm_quartiere') }} dq
        ON oq.nome = dq.nome_quartiere

GROUP BY c.nome, c.latitudine, c.longitudine, c.update_time