{{ config(
    materialized='incremental',
    unique_key=['idData'],
    alias='dm_data'
) }}

{% set initialize %}
    CREATE SEQUENCE IF NOT EXISTS seq_dm_data;
{% endset %}
{% do run_query(initialize) %}

WITH source_data AS (
    -- Prepariamo i dati unici dalla ODS (Source)
    SELECT
        CAST(data AS DATE) as data,
        EXTRACT(YEAR FROM data) as anno,
        EXTRACT(MONTH FROM data) as mese,
        EXTRACT(DAY FROM data) as giorno,

        CASE
            WHEN EXTRACT(ISODOW FROM data) IN (6, 7) THEN 1
            ELSE 0
        END as is_weekend,

        -- Prendiamo l'ultimo aggiornamento disponibile per questa data
        MAX(update_time) as update_time

    FROM {{ ref('ods_rilevazione') }}
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    -- Logica Sequence: Se esiste già prendi l'ID vecchio, altrimenti generane uno nuovo
    {% if is_incremental() %}
        IFNULL(target.idData, nextval('seq_dm_data'))
    {% else %}
        nextval('seq_dm_data')
    {% endif %} as idData,

    d.data,
    d.anno,
    d.mese,
    d.giorno,
    d.is_weekend,

FROM source_data as d
    -- Join per recuperare l'ID se esiste già
    LEFT JOIN {{ this }} as target ON d.data = target.data

{% if is_incremental() %}
    WHERE d.update_time > (
        SELECT COALESCE(MAX(time), '1900-01-01 00:00:00')
        FROM last_execution_times
        WHERE target_table = '{{ this.identifier }}'
    )
{% endif %}