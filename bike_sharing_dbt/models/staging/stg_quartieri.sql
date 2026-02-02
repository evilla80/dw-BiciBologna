{{ config(materialized='view') }}

SELECT
    -- 1. Nome Quartiere
    quartiere as nome_quartiere,

    -- 2. Estrazione Centroide (Punto)
    -- geo_point_2d nel JSON è una struct {lat: ..., lon: ...}
    ST_Point(geo_point_2d.lon, geo_point_2d.lat) as centroide,

    -- 3. Estrazione Perimetro (Poligono)
    -- geo_shape è una struct {type: Feature, geometry: {...}}.
    -- Dobbiamo prendere la 'geometry' interna e convertirla in testo JSON per la funzione spaziale.
    ST_GeomFromGeoJSON(to_json(geo_shape.geometry)) as perimetro

FROM {{ source('staging', 'raw_quartieri') }}