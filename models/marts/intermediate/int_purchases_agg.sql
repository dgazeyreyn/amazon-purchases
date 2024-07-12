with agg as (

    select
        survey_responseid,
        count(*) as purchases,
        count(distinct order_date) as order_dates,
        sum(purchase_price_per_unit) as total_purchase_price_per_unit,
        sum(Quantity) as total_quantity,
        sum(purchase_price_per_unit * Quantity) as total_spend,
        count(distinct asin_isbn_product_code) as products,
        count(distinct Category) as categories
    from
        {{ ref('stg_purchases__amazon_purchases') }}
    group by
        survey_responseid

)

select * from agg