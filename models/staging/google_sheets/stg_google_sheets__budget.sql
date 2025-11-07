{{
  config(
    materialized='incremental',
    unique_key = '_row'
  )
}}

WITH src_budget AS (
    SELECT *
    FROM {{ source('google_sheets', 'budget') }}
    {% if is_incremental() %}
    WHERE _fivetran_synced > (SELECT MAX(DATE_LOAD) FROM {{ this }})
    {% endif %}
),
renamed_casted AS (
    SELECT
        _row
        , product_id
        , quantity
        , month
        , _fivetran_synced AS DATE_LOAD
    FROM src_budget
    )

SELECT * FROM renamed_casted