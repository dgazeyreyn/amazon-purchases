with

source as (

    select * from {{ source('purchases', 'amazon_purchases') }}

),

transformed as (

    select 

        *,
        extract(year from order_date) as order_year,
        case
            when order_date in ('2018-07-16', '2018-07-17', '2019-07-15', '2019-07-16', '2020-10-13', '2020-10-14',
            '2021-06-21', '2021-06-22', '2022-07-12', '2022-07-13') then true
            else false
        end as amazon_prime_day

    from source

)

select * from transformed

-- 5,027 distinct survey respondents
-- 1,850,717 total records
-- 89,458 records with NULL Category
-- 973 records with NULL asin_isbn_product_code