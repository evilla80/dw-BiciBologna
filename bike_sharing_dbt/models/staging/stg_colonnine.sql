{{ config(materialized='view') }}

SELECT
    CAST(data AS DATE) as data_rilevazione,
    CAST(data AS TIMESTAMP) as timestamp_completo,
    EXTRACT(HOUR FROM CAST(data AS TIMESTAMP)) as ora_rilevazione,

    -- Attributi Colonnina
    colonnina as nome_colonnina,
    CAST(split_part(geo_point_2d, ',', 1) AS DOUBLE) as latitudine,
    CAST(split_part(geo_point_2d, ',', 2) AS DOUBLE) as longitudine,

    CAST(direzione_centro AS INTEGER) as conteggio_centro,
    CAST(direzione_periferia AS INTEGER) as conteggio_periferia,
    CAST(totale AS INTEGER) as conteggio_totale

FROM {{ source('staging', 'raw_colonnine') }}