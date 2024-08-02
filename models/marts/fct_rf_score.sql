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
    )
select
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
    count(*) as users,
    avg(recency) as average_recency,
    avg(purchases) as average_frequency,
    avg(total_spend) as average_monetary
from quintiles
group by 1, 2
