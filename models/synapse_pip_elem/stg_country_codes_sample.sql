{{ config(materialized='table') }}

with source_data as (
    select
        [Country],
        [ISO Code],
        [Dialing Code]
    from {{ source('synapse_dqlabs', 'country_codes_sample') }}
),

renamed as (
    select
        Country as country,
        [ISO Code] as iso_code,
        [Dialing Code] as dialing_code
    from source_data
)

select *
from renamed;
