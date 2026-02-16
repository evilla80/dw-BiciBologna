{{ config(materialized='incremental', unique_key='nome', alias='quartiere') }}

SELECT
    nome,
    latitudineCentro,
    longitudineCentro,
    superficie,
    perimetro,
    insert_time,
    update_time

FROM {{ ref('ods_quartiere_transform') }}
