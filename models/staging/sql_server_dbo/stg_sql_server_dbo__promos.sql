{{
  config(
    materialized='table'
  )
}}

WITH src_promo AS (
    SELECT *
    FROM {{ source('sql_server_dbo','promos')}}
),
transform_promo AS (
    SELECT 
        md5(PROMO_ID) AS PROMO_ID,
        PROMO_ID AS PROMO_NAME,
        DISCOUNT AS DISCOUNT_DOLLAR,
        STATUS AS PROMO_STATUS,
        _FIVETRAN_DELETED,
        {{ convert_time_utc('_FIVETRAN_SYNCED') }} AS DATA_UPDATED_UTC
    FROM 
        src_promo
    WHERE 
        _FIVETRAN_DELETED IS NULL
    UNION ALL
        SELECT  
            md5('no promo') AS PROMO_ID,
            'no promo' AS PROMO_NAME,
            0 AS DISCOUNT_DOLLAR,
            'inactive' AS PROMO_STATUS,
            NULL AS _FIVETRAN_DELETED,
            NULL AS _FIVETRAN_SYNCED
)

SELECT * FROM transform_promo