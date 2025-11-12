{{
  config(
    materialized='view'
  )
}}

WITH src_users AS (
    SELECT 
        MD5(USER_ID) AS USER_ID,
        {{ convert_time_utc('UPDATED_AT') }} AS UPDATED_AT_UTC,
        MD5(ADDRESS_ID) AS ADDRESS_ID,
        LAST_NAME,
        {{ convert_time_utc('CREATED_AT') }} AS CREATED_AT_UTC,
        PHONE_NUMBER,
        FIRST_NAME,
        TOTAL_ORDERS,
        EMAIL,
        _FIVETRAN_DELETED,
        {{ convert_time_utc('_FIVETRAN_SYNCED') }} AS DATA_UPDATED_UTC
    FROM {{ source('sql_server_dbo','users')}}
    WHERE _FIVETRAN_DELETED IS NULL
)

SELECT * FROM src_users