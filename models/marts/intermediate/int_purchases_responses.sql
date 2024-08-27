{{ config(materialized='table') }}

with survey as (
    
    select * from {{ ref("stg_surveys__responses") }}
    
),

purchases as (
    
    select * from {{ ref("stg_purchases__amazon_purchases") }}
    
),

survey_purchases as (

select
    survey.*,
    purchases.order_date,
    purchases.purchase_price_per_unit,
    purchases.quantity,
    purchases.title,
    purchases.asin_isbn_product_code,
    purchases.category,
    purchases.order_year,
    purchases.amazon_prime_day,
    purchases.total_spend
from
    survey
left join
    purchases on purchases.survey_responseid = survey.survey_responseid

)

-- 1,850,717 total records

select * from survey_purchases
    

