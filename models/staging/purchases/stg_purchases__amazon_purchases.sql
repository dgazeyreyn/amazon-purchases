with

source as (

    select * from {{ source('purchases', 'amazon_purchases') }}

),

transformed as (

    select 

        *

    from source

)

select * from transformed