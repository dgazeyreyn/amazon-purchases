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

-- 5,027 distinct survey respondents
-- 1,850,717 total records
-- 89,458 records with NULL Category
-- 973 records with NULL asin_isbn_product_code