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
    ),
    recency_analysis as (
        select
            recency_quintile as quintile,
            'recency' as metric,
            q_demos_age as demo_band,
            'age' as demo,
            count(*) as cell_counts
        from quintiles
        group by 1, 2, 3, 4
        union all
        select
            recency_quintile as quintile,
            'recency' as metric,
            q_demos_race as demo_band,
            'race' as demo,
            count(*) as cell_counts
        from quintiles
        group by 1, 2, 3, 4
        union all
        select
            recency_quintile as quintile,
            'recency' as metric,
            q_demos_education as demo_band,
            'education' as demo,
            count(*) as cell_counts
        from quintiles
        group by 1, 2, 3, 4
        union all
        select
            recency_quintile as quintile,
            'recency' as metric,
            q_demos_income as demo_band,
            'income' as demo,
            count(*) as cell_counts
        from quintiles
        group by 1, 2, 3, 4
        union all
        select
            recency_quintile as quintile,
            'recency' as metric,
            q_demos_gender as demo_band,
            'gender' as demo,
            count(*) as cell_counts
        from quintiles
        group by 1, 2, 3, 4
    ),
    frequency_analysis as (
        select
            frequency_quintile as quintile,
            'frequency' as metric,
            q_demos_age as demo_band,
            'age' as demo,
            count(*) as cell_counts
        from quintiles
        group by 1, 2, 3, 4
        union all
        select
            frequency_quintile as quintile,
            'frequency' as metric,
            q_demos_race as demo_band,
            'race' as demo,
            count(*) as cell_counts
        from quintiles
        group by 1, 2, 3, 4
        union all
        select
            frequency_quintile as quintile,
            'frequency' as metric,
            q_demos_education as demo_band,
            'education' as demo,
            count(*) as cell_counts
        from quintiles
        group by 1, 2, 3, 4
        union all
        select
            frequency_quintile as quintile,
            'frequency' as metric,
            q_demos_income as demo_band,
            'income' as demo,
            count(*) as cell_counts
        from quintiles
        group by 1, 2, 3, 4
        union all
        select
            frequency_quintile as quintile,
            'frequency' as metric,
            q_demos_gender as demo_band,
            'gender' as demo,
            count(*) as cell_counts
        from quintiles
        group by 1, 2, 3, 4
    ),
    stack as (
        select *
        from recency_analysis
        union all
        select *
        from frequency_analysis
    ),
    row_total as (
        select q_demos_age as demo_band, 'age' as demo, count(*) as row_sum
        from quintiles
        group by 1, 2
        union all
        select q_demos_race as demo_band, 'race' as demo, count(*) as row_sum
        from quintiles
        group by 1, 2
        union all
        select q_demos_education as demo_band, 'education' as demo, count(*) as row_sum
        from quintiles
        group by 1, 2
        union all
        select q_demos_income as demo_band, 'income' as demo, count(*) as row_sum
        from quintiles
        group by 1, 2
        union all
        select q_demos_gender as demo_band, 'gender' as demo, count(*) as row_sum
        from quintiles
        group by 1, 2
    ),
    column_total as (
        select frequency_quintile as quintile, count(*) as column_sum
        from quintiles
        group by 1
    ),
    overall as (
        select count(*) as overall_total from `dbt_dreynolds.fct_user_purchase_metrics`
    )
select s.*, r.row_sum, c.column_sum, o.overall_total
from stack s
left join row_total r on r.demo = s.demo and r.demo_band = s.demo_band
left join column_total c on c.quintile = s.quintile
cross join overall o