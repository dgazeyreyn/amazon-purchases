{{ config(materialized='table') }}

with responses as (

    select * from {{ ref('stg_surveys__responses') }}

),

purchases_agg as (

    select * from {{ ref('int_purchases_agg') }}

),

final as (

select
    responses.*,
    purchases_agg.purchases,
    purchases_agg.order_dates,
    purchases_agg.total_purchase_price_per_unit,
    purchases_agg.total_quantity,
    purchases_agg.total_spend,
    purchases_agg.products,
    purchases_agg.categories,
    purchases_agg.first_order_date,
    purchases_agg.last_order_date
from
    responses
left join
    purchases_agg on responses.survey_responseid = purchases_agg.survey_responseid

)

select * from final