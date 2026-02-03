{{ config(
    materialized='incremental',
    unique_key=['idColonnina'],
    alias='dm_colonnina'
) }}

{% set initialize %}
    CREATE SEQUENCE IF NOT EXISTS seq_dm_colonnina;
{% endset %}
{% do run_query(initialize) %}

SELECT
    {% if is_incremental() %}
        IFNULL(target.idColonnina, nextval('seq_dm_colonnina'))
    {% else %}
        nextval('seq_dm_colonnina')
    {% endif %} as idColonnina,

    source.nome_colonnina,
    source.latitudine,
    source.longitudine,
    source.quartiere_idQuartiere,

    current_timestamp as insert_time,
    current_timestamp as update_time

FROM {{ ref('dm_colonnina_lookup') }} as source

{% if is_incremental() %}
    LEFT JOIN {{ this }} as target
    ON source.nome_colonnina = target.nome_colonnina
    WHERE source.insert_time > (SELECT MAX(update_time) FROM {{ this }})
{% endif %}