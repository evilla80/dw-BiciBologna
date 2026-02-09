{{ config(
    materialized='incremental',
    unique_key=['id_data'],
    alias='dm_data'
) }}

{% set initialize %}
    CREATE SEQUENCE IF NOT EXISTS seq_dm_data;
{% endset %}
{% do run_query(initialize) %}

WITH source_data AS (
    -- Prepariamo i dati unici dalla ODS (Source)
    SELECT
        r.data,
        EXTRACT(YEAR FROM r.data) as anno,
        EXTRACT(MONTH FROM r.data) as mese,
        EXTRACT(DAY FROM r.data) as giorno,

        CASE
            WHEN EXTRACT(ISODOW FROM data) IN (6, 7) THEN 1
            ELSE 0
        END as is_weekend,

         CASE
            WHEN EXTRACT(MONTH FROM r.data) = 1 AND EXTRACT(DAY FROM r.data) = 1 THEN 1
            WHEN EXTRACT(MONTH FROM r.data) = 1 AND EXTRACT(DAY FROM r.data) = 6 THEN 1
            WHEN EXTRACT(MONTH FROM r.data) = 4 AND EXTRACT(DAY FROM r.data) = 25 THEN 1
            WHEN EXTRACT(MONTH FROM r.data) = 5 AND EXTRACT(DAY FROM r.data) = 1 THEN 1
            WHEN EXTRACT(MONTH FROM r.data) = 6 AND EXTRACT(DAY FROM r.data) = 2 THEN 1
            WHEN EXTRACT(MONTH FROM r.data) = 8 AND EXTRACT(DAY FROM r.data) = 15 THEN 1
            WHEN EXTRACT(MONTH FROM r.data) = 11 AND EXTRACT(DAY FROM r.data) = 1 THEN 1
            WHEN EXTRACT(MONTH FROM r.data) = 12 AND EXTRACT(DAY FROM r.data) = 8 THEN 1
            WHEN EXTRACT(MONTH FROM r.data) = 12 AND EXTRACT(DAY FROM r.data) = 25 THEN 1
            WHEN EXTRACT(MONTH FROM r.data) = 12 AND EXTRACT(DAY FROM r.data) = 26 THEN 1
            ELSE 0
        END as is_festivo,

        -- Prendiamo l'ultimo aggiornamento disponibile per questa data
        MAX(update_time) as update_time

    FROM {{ ref('ods_rilevazione') }} as r
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    -- Logica Sequence: Se esiste già prendi l'ID vecchio, altrimenti generane uno nuovo
    {% if is_incremental() %}
        IFNULL(target.id_data, nextval('seq_dm_data'))
    {% else %}
        nextval('seq_dm_data')
    {% endif %} as id_data,

    d.data,
    d.anno,
    d.mese,
    d.giorno,
    d.is_weekend,
    d.is_festivo

FROM source_data as d
{% if is_incremental() %}
    LEFT JOIN {{ this }} as target ON d.data = target.data
{% endif %}
{% if is_incremental() %}

    WHERE d.update_time > (
        SELECT COALESCE(MAX(time), '1900-01-01 00:00:00')
        FROM last_execution_times
        WHERE target_table = '{{ this.identifier }}'
    )
{% endif %}