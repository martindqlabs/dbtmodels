{{ config(materialized='view') }}
SELECT 
   city,
   state_name,
   divison
FROM {{ ref('bankdistricttable') }}