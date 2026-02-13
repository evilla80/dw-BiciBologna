{{ config(materialized='ephemeral') }}

WITH calcolo_metrica AS (
    SELECT
        c.nome as nome_colonnina,
        c.latitudine,
        c.longitudine,
        c.update_time,
        dq.id_quartiere,

        ST_Distance(
            ST_Point(c.longitudine, c.latitudine),
            ST_Point(dq.longitudineCentro, dq.latitudineCentro)
        ) as distanza

    FROM {{ ref('ods_colonnina') }} c
    
    LEFT JOIN {{ ref('ods_quartiere') }} oq
        ON ST_DWithin(ST_Point(c.longitudine, c.latitudine), oq.perimetro, 0.0002)

    LEFT JOIN {{ ref('dm_quartiere') }} dq
        ON oq.nome = dq.nome
),

classifica AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(
            PARTITION BY nome_colonnina 
            ORDER BY distanza ASC
        ) as ranking_vicinanza
    FROM calcolo_metrica
)

SELECT
    nome_colonnina,
    latitudine,
    longitudine,
    update_time,
    id_quartiere as quartiere_idQuartiere
FROM classifica
WHERE ranking_vicinanza = 1