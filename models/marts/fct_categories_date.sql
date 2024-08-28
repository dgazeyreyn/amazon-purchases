{{ config(materialized="table") }}
with
    purchases as (select * from {{ ref("stg_purchases__amazon_purchases") }}),

    final as (
        select
            order_date,
            category,
            amazon_prime_day,
            'individual category' as aggregation,
            count(distinct survey_responseid) as users,
            count(*) as purchases,
            sum(total_spend) as total_spend
        from purchases
        group by 1, 2, 3, 4
        union all
        select
            order_date,
            'all' as category,
            amazon_prime_day,
            'all categories' as aggregation,
            count(distinct survey_responseid) as users,
            count(*) as purchases,
            sum(total_spend) as total_spend
        from purchases
        group by 1, 2, 3, 4

    )
select *
from final
