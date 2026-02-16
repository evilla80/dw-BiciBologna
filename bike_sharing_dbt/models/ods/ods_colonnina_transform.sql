{{ config(materialized='ephemeral') }}

SELECT DISTINCT
    colonnina as nome,
    CAST(split_part(geo_point_2d, ',', 1) AS DOUBLE) as latitudine,
    CAST(split_part(geo_point_2d, ',', 2) AS DOUBLE) as longitudine,
    CAST(data AS TIMESTAMP) AS timestamp_completo,
    current_timestamp AS insert_time,
    current_timestamp AS update_time

FROM {{ source('staging', 'raw_colonnine') }}