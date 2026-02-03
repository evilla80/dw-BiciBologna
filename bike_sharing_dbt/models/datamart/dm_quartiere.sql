{{ config(
    materialized='incremental',
    unique_key=['idQuartiere'],
    alias='dm_quartiere'
) }}

{% set initialize %}
    CREATE SEQUENCE IF NOT EXISTS seq_dm_quartiere;
{% endset %}
{% do run_query(initialize) %}

SELECT
    -- Se è incrementale (tabella esiste), prova a recuperare l'ID vecchio.
    -- Se è la prima volta (o tabella non trovata), genera nuovo ID.
    {% if is_incremental() %}
        IFNULL(target.idQuartiere, nextval('seq_dm_quartiere'))
    {% else %}
        nextval('seq_dm_quartiere')
    {% endif %} as idQuartiere,

    source.nome as nome_quartiere,
    source.superficie,
    source.perimetro,
    source.latitudine_centro,
    source.longitudine_centro,

    current_timestamp as insert_time,
    current_timestamp as update_time

FROM {{ ref('ods_quartiere') }} as source

-- JOIN PROTETTA:
-- Viene eseguita SOLO se siamo in modalità incrementale (la tabella esiste già)
{% if is_incremental() %}
    LEFT JOIN {{ this }} as target
    ON source.nome = target.nome_quartiere
    WHERE source.insert_time > (SELECT MAX(update_time) FROM {{ this }})
{% endif %}