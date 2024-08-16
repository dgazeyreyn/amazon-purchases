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
            1
            + (rank() over (order by recency desc) - 1)
            * 5
            / count(1) over (partition by (select 1)) as recency_ntile,
            1
            + (rank() over (order by purchases) - 1)
            * 5
            / count(1) over (partition by (select 1)) as frequency_ntile,
            1
            + (rank() over (order by total_spend) - 1)
            * 5
            / count(1) over (partition by (select 1)) as monetary_ntile,
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
    percentiles as (
        select
            *,
            percentile_cont(recency, 0.5) over (
                partition by recency_quintile
            ) as recency_median,
            percentile_cont(purchases, 0.5) over (
                partition by frequency_quintile
            ) as frequency_median,
            percentile_cont(total_spend, 0.5) over (
                partition by monetary_quintile
            ) as monetary_median
        from quintile_assignments
    )
select
    recency_quintile,
    frequency_quintile,
    any_value(recency_median) as recency_median,
    any_value(frequency_median) as frequency_median,
    avg(total_spend) as average_spend
from percentiles
group by 1, 2
order by 1, 2
