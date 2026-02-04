/*
    Normalizzazione anagrafica Colonnine (Transform)
    Estrae le colonnine uniche dai dati grezzi e parsa le coordinate.
*/

{{ config(materialized='ephemeral') }}

SELECT DISTINCT
    colonnina as nome,
    -- Parsing della stringa "lat, lon" (formato: "44.123, 11.456")
    CAST(split_part(geo_point_2d, ',', 1) AS DOUBLE) as latitudine,
    CAST(split_part(geo_point_2d, ',', 2) AS DOUBLE) as longitudine,

    current_timestamp AS insert_time,
    current_timestamp AS update_time
FROM {{ source('staging', 'raw_colonnine') }}