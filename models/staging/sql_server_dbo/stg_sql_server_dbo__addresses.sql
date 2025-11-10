{{
  config(
    materialized='view'
  )
}}

WITH src_addresses AS (
    SELECT DISTINCT
        MD5(ADDRESS_ID) AS ADDRESS_ID,
        ADDRESS AS ADDRESS_NAME,
        REGEXP_REPLACE(ADDRESS, '[0-9]+\\s*', '') AS STREET,
        MD5(ZIPCODE) AS ZIPCODE_ID,
        _FIVETRAN_DELETED AS _FIVETRAN_DELETED,
        {{ convert_time_utc('_FIVETRAN_SYNCED') }} AS DATA_UPDATED_UTC
    FROM {{ source('sql_server_dbo','addresses')}}
    WHERE _FIVETRAN_DELETED IS NULL
)

SELECT * FROM src_addresses