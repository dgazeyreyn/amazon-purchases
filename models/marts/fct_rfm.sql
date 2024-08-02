{{ config(materialized="table") }}
with
    latest_date as (
        select a.*, b.recency_date
        from `dbt_dreynolds.fct_user_purchase_metrics` a
        cross join
            (
                select max(last_order_date) as recency_date
                from `dbt_dreynolds.fct_user_purchase_metrics`
            ) b
    ),
    recency_days as (
        select *, date_diff(latest_date.recency_date, last_order_date, day) as recency
        from latest_date
    ),
    quintiles as (
        select
            *,
            ntile(5) over (order by recency desc) as recency_quintile,
            ntile(5) over (order by purchases) as frequency_quintile,
        from recency_days
    )
select
    concat(recency_quintile, ',', frequency_quintile) as rf_segment,
    recency_quintile,
    frequency_quintile,
    sum(total_spend) as monetary_value,
    count(*) as users
from quintiles
group by 1, 2, 3
