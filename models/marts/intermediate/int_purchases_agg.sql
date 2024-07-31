with purchases as (

    select * from {{ ref('stg_purchases__amazon_purchases') }}

),

agg as (

    select
        survey_responseid,
        count(*) as purchases,
        count(distinct order_date) as order_dates,
        sum(purchase_price_per_unit) as total_purchase_price_per_unit,
        sum(Quantity) as total_quantity,
        sum(purchase_price_per_unit * Quantity) as total_spend,
        count(distinct asin_isbn_product_code) as products,
        count(distinct Category) as categories,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from
        purchases
    where
        order_date <= '2022-12-31'
    group by
        survey_responseid

)

select * from agg