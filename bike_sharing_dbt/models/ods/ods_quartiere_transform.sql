{{ config(materialized='ephemeral') }}

SELECT
    quartiere AS nome,
    CAST(json_extract(to_json(geo_point_2d), '$.lat') AS DOUBLE) AS latitudineCentro,
    CAST(json_extract(to_json(geo_point_2d), '$.lon') AS DOUBLE) AS longitudineCentro,
    CAST(ST_Area(ST_GeomFromGeoJSON(json_extract(to_json(geo_shape), '$.geometry'))) AS DOUBLE) AS superficie,
    ST_GeomFromGeoJSON(json_extract(to_json(geo_shape), '$.geometry')) AS perimetro,
    current_timestamp AS insert_time,
    current_timestamp AS update_time

FROM {{ source('staging', 'raw_quartieri') }}