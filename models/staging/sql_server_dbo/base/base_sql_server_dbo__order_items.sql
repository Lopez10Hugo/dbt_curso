{{
  config(
    materialized='incremental',
    on_schema_change = 'fail',
    unique_key = 'ORDER_ITEMS_ID'
  )
}}


WITH src_order_items AS (
    SELECT *
    FROM {{ source('sql_server_dbo','order_items')}}
    {% if is_incremental() %}
    WHERE _fivetran_synced > (SELECT MAX(DATA_UPDATED_UTC) FROM {{ this }})
    {% endif %}
),
order_items_transform AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['ORDER_ID', 'PRODUCT_ID']) }} AS ORDER_ITEMS_ID,
        MD5(ORDER_ID) AS ORDER_ID,
        MD5(PRODUCT_ID) AS PRODUCT_ID,
        QUANTITY,
        _FIVETRAN_DELETED,
        {{ convert_time_utc('_FIVETRAN_SYNCED') }} AS DATA_UPDATED_UTC
    FROM {{ source('sql_server_dbo','order_items')}} oi
)

SELECT * FROM order_items_transform