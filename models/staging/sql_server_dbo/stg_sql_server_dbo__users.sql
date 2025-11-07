{{
  config(
    materialized='view'
  )
}}

WITH src_users AS (
    SELECT 
        USER_ID,
        UPDATED_AT,
        ADDRESS_ID,
        LAST_NAME,
        CREATED_AT,
        PHONE_NUMBER,
        TOTAL_ORDERS,
        FIRST_NAME,
        EMAIL,
        _FIVETRAN_DELETED,
        _FIVETRAN_SYNCED
    FROM {{ source('sql_server_dbo','users')}}
)

SELECT * FROM src_users