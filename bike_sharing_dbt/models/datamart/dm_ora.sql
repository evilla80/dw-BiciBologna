{{ config(
    materialized='incremental',
    unique_key=['id_ora'],
    alias='dm_ora'
) }}

{% set initialize %}
    CREATE SEQUENCE IF NOT EXISTS seq_dm_ora;
{% endset %}
{% do run_query(initialize) %}

WITH source_data AS (
    -- Prepariamo i dati unici dalla ODS (Source)
    SELECT
        ora,
        CASE
            WHEN ora BETWEEN 0 AND 5 THEN 'Notte'
            WHEN ora BETWEEN 6 AND 11 THEN 'Mattina'
            WHEN ora BETWEEN 12 AND 17 THEN 'Pomeriggio'
            ELSE 'Sera'
        END as fascia_oraria,

        -- Prendiamo l'ultimo aggiornamento disponibile per quest'ora
        MAX(update_time) as update_time

    FROM {{ ref('ods_rilevazione') }}
    GROUP BY 1, 2
)

SELECT
    {% if is_incremental() %}
        IFNULL(target.id_ora, nextval('seq_dm_ora'))
    {% else %}
        nextval('seq_dm_ora')
    {% endif %} as id_ora,

    o.ora,
    o.fascia_oraria


FROM source_data as o
{% if is_incremental() %}
    LEFT JOIN {{ this }} as target ON o.ora = target.ora
{% endif %}

{% if is_incremental() %}
    WHERE o.update_time > (
        SELECT COALESCE(MAX(time), '1900-01-01 00:00:00')
        FROM last_execution_times
        WHERE target_table = '{{ this.identifier }}'
    )
{% endif %}