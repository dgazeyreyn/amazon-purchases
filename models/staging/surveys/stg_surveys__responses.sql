with

source as (

    select * from {{ source('surveys', 'survey') }}

),

transformed as (

    select 

        *

    from source

)

select * from transformed

-- 5,027 distinct survey responses