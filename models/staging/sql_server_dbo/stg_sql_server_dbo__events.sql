{{
  config(
    materialized='view'
  )
}}

WITH src_events AS (
    SELECT 
        *
    FROM {{ source('sql_server_dbo','events')}}
),
events_transform AS (
    SELECT
        md5(EVENT_ID) AS EVENT_ID,
        PAGE_URL,
        EVENT_TYPE,
        md5(USER_ID) AS USER_ID,
        md5(PRODUCT_ID) AS PRODUCT_ID,
        SESSION_ID,
        CONVERT_TIMEZONE('UTC',CREATED_AT) AS CREATED_AT_UTC,
        MD5(ORDER_ID) AS ORDER_ID,
        _FIVETRAN_DELETED,
        CONVERT_TIMEZONE('UTC',_FIVETRAN_SYNCED) AS DATA_UPDATED_UTC
    FROM src_events
)

SELECT * FROM src_events