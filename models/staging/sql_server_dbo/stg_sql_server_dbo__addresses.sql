{{
  config(
    materialized='view'
  )
}}

WITH src_budget AS (
    SELECT *
    FROM {{ source('sql_server_dbo','addresses')}}
)

SELECT * FROM src_budget;