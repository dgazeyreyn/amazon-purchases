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
            ntile(5) over (order by total_spend) as monetary_quintile
        from recency_days
    )
select
    recency_quintile as quintile,
    'recency' as metric,
    count(*) as users,
    min(recency) as minimum,
    max(recency) as maximum,
    avg(recency) as average
from quintiles
group by 1, 2
union all
select
    frequency_quintile as quintile,
    'frequency' as metric,
    count(*) as users,
    min(purchases) as minimum,
    max(purchases) as maximum,
    avg(purchases) as average
from quintiles
group by 1, 2
union all
select
    monetary_quintile as quintile,
    'monetary' as metric,
    count(*) as users,
    min(total_spend) as minimum,
    max(total_spend) as maximum,
    avg(total_spend) as average
from quintiles
group by 1, 2
