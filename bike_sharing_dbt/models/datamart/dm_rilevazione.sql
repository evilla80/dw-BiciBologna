{{ config(materialized='incremental', unique_key=['data_idData', 'ora_idOra', 'colonnina_idColonnina'],alias='dm_rilevazione') }}

SELECT
    r.data_idData,
    r.ora_idOra,
    r.colonnina_idColonnina,
    r.meteo_idMeteo,
    r.direzioneCentro,
    r.direzionePeriferia,
    r.totale

FROM {{ ref('dm_rilevazione_fk_lookup') }} as r

{% if is_incremental() %}
    WHERE r.timestamp_completo > (
        SELECT COALESCE(MAX(time), '1900-01-01 00:00:00')
        FROM last_execution_times
        WHERE target_table = '{{ this.identifier }}'
    )
{% endif %}