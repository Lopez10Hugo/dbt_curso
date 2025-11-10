{{
  config(
    materialized='view'
  )
}}

WITH inventory_transform AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key([
            convert_time_utc('_FIVETRAN_SYNCED'),
            'PRODUCT_ID'
        ]) }} AS INVENTORY_ID,
        MD5(PRODUCT_ID) AS PRODUCT_ID,
        INVENTORY AS INVENTORY_QUANTITY,
        _FIVETRAN_DELETED,
        DAY({{ convert_time_utc('_FIVETRAN_SYNCED') }}) AS DAY_UPDATED,
       {{ convert_time_utc('_FIVETRAN_SYNCED') }} AS DATA_UPDATED_UTC
    FROM {{ source('sql_server_dbo','products')}}
    WHERE _FIVETRAN_DELETED IS NULL
)

SELECT * FROM inventory_transform