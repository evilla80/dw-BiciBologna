{{ config(materialized='ephemeral') }}

WITH raw_data AS (
    SELECT
        data AS data_rilevazione,
        CAST(data AS TIMESTAMP) AS timestamp_completo,
        colonnina AS nome_colonnina,
        -- Parsing della stringa "lat, lon" delle colonnine
        CAST(split_part(geo_point_2d, ',', 1) AS DOUBLE) AS latitudine,
        CAST(split_part(geo_point_2d, ',', 2) AS DOUBLE) AS longitudine,
        direzione_centro AS direzioneCentro,
        direzione_periferia AS direzionePeriferia,
        totale,
        ST_Point(
            CAST(split_part(geo_point_2d, ',', 2) AS DOUBLE),
            CAST(split_part(geo_point_2d, ',', 1) AS DOUBLE)
        ) AS geom_punto
    FROM {{ source('staging', 'raw_colonnine') }}
),

quartieri AS (
    SELECT
        quartiere AS nome,
        -- 1. Estrazione Geometria (Funziona già)
        ST_GeomFromGeoJSON(json_extract(to_json(geo_shape), '$.geometry')) AS geom_perimetro,

        -- 2. Estrazione Coordinate Centroide (Metodo JSON Blindato)
        -- Convertiamo la struct in testo JSON ed estraiamo i valori con la chiave
        CAST(json_extract(to_json(geo_point_2d), '$.lat') AS DOUBLE) AS latitudine_centro,
        CAST(json_extract(to_json(geo_point_2d), '$.lon') AS DOUBLE) AS longitudine_centro

    FROM {{ source('staging', 'raw_quartieri') }}
)

SELECT
    r.timestamp_completo,
    r.data_rilevazione,
    r.nome_colonnina,
    r.latitudine,
    r.longitudine,
    r.direzioneCentro,
    r.direzionePeriferia,
    r.totale,

    q.nome AS nome_quartiere,
    CAST(ST_Area(q.geom_perimetro) AS DOUBLE) AS superficie,
    CAST(ST_Perimeter(q.geom_perimetro) AS DOUBLE) AS perimetro,

    q.latitudine_centro,
    q.longitudine_centro,

    current_timestamp AS extraction_time
FROM raw_data r
LEFT JOIN quartieri q
ON ST_Within(r.geom_punto, q.geom_perimetro)