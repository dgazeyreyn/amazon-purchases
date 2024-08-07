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
    ntiles as (
        select
            *,
            1+(rank() over (order by recency desc)-1) * 5 / count(1) over (partition by (select 1)) as recency_ntile,
            1+(rank() over (order by purchases)-1) * 5 / count(1) over (partition by (select 1)) as frequency_ntile,
            1+(rank() over (order by total_spend)-1) * 5 / count(1) over (partition by (select 1)) as monetary_ntile,
        from recency_days
    ),
    quintile_assignments as (
        select
            *,
            case
                when recency_ntile >= 1 and recency_ntile < 2
                then 1
                when recency_ntile >= 2 and recency_ntile < 3
                then 2
                when recency_ntile >= 3 and recency_ntile < 4
                then 3
                when recency_ntile >= 4 and recency_ntile < 5
                then 4
                when recency_ntile >= 5
                then 5
            end as recency_quintile,
            case
                when frequency_ntile >= 1 and frequency_ntile < 2
                then 1
                when frequency_ntile >= 2 and frequency_ntile < 3
                then 2
                when frequency_ntile >= 3 and frequency_ntile < 4
                then 3
                when frequency_ntile >= 4 and frequency_ntile < 5
                then 4
                when frequency_ntile >= 5
                then 5
            end as frequency_quintile,
            case
                when monetary_ntile >= 1 and monetary_ntile < 2
                then 1
                when monetary_ntile >= 2 and monetary_ntile < 3
                then 2
                when monetary_ntile >= 3 and monetary_ntile < 4
                then 3
                when monetary_ntile >= 4 and monetary_ntile < 5
                then 4
                when monetary_ntile >= 5
                then 5
            end as monetary_quintile
        from ntiles
    ),
    medians as (
    select
        *,
        PERCENTILE_CONT(recency, 0.5) OVER(PARTITION BY recency_quintile) AS recency_median,
        PERCENTILE_CONT(purchases, 0.5) OVER(PARTITION BY frequency_quintile) AS frequency_median,
        PERCENTILE_CONT(total_spend, 0.5) OVER(PARTITION BY monetary_quintile) AS monetary_median,
    from
        quintile_assignments
    )
select
    recency_quintile as quintile,
    'recency' as metric,
    count(*) as users,
    min(recency) as minimum,
    max(recency) as maximum,
    avg(recency) as average,
    any_value(recency_median) as median
from medians
group by 1, 2
union all
select
    frequency_quintile as quintile,
    'frequency' as metric,
    count(*) as users,
    min(purchases) as minimum,
    max(purchases) as maximum,
    avg(purchases) as average,
    any_value(frequency_median) as median
from medians
group by 1, 2
union all
select
    monetary_quintile as quintile,
    'monetary' as metric,
    count(*) as users,
    min(total_spend) as minimum,
    max(total_spend) as maximum,
    avg(total_spend) as average,
    any_value(monetary_median) as median
from medians
group by 1, 2
