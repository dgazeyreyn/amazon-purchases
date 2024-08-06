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
    recency_analysis as (
        select
            recency_quintile as quintile,
            'recency' as metric,
            q_demos_age as demo_band,
            'age' as demo,
            count(*) as cell_counts
        from quintile_assignments
        group by 1, 2, 3, 4
        union all
        select
            recency_quintile as quintile,
            'recency' as metric,
            case
                when q_demos_race = 'American Indian/Native American or Alaska Native'
                then 'American Indian/Native American or Alaska Native Alone'
                when q_demos_race = 'Asian'
                then 'Asian Alone'
                when q_demos_race = 'Black or African American'
                then 'Black or African American Alone'
                when q_demos_race = 'Native Hawaiian or Other Pacific Islander'
                then 'Native Hawaiian or Other Pacific Islander Alone'
                when q_demos_race = 'Other'
                then 'Other'
                when q_demos_race = 'White or Caucasian'
                then 'White or Caucasian Alone'
                else 'Multiracial'
            end as demo_band,
            'race' as demo,
            count(*) as cell_counts
        from quintile_assignments
        group by 1, 2, 3, 4
        union all
        select
            recency_quintile as quintile,
            'recency' as metric,
            case
                when q_demos_education = 'Some high school or less'
                then '1 - Some high school or less'
                when q_demos_education = 'High school diploma or GED'
                then '2 - High school diploma or GED'
                when q_demos_education = 'Prefer not to say'
                then '5 - Prefer not to say'
                when
                    q_demos_education
                    = 'Graduate or professional degree (MA, MS, MBA, PhD, JD, MD, DDS, etc)'
                then
                    '4 - Graduate or professional degree (MA, MS, MBA, PhD, JD, MD, DDS, etc)'
                else '3 - Bachelors degree'
            end as demo_band,
            'education' as demo,
            count(*) as cell_counts
        from quintile_assignments
        group by 1, 2, 3, 4
        union all
        select
            recency_quintile as quintile,
            'recency' as metric,
            case
                when q_demos_income = 'Less than $25,000'
                then '1 - Less than $25,000'
                when q_demos_income = '$25,000 - $49,999'
                then '2 - $25,000 - $49,999'
                when q_demos_income = '$50,000 - $74,999'
                then '3 - $50,000 - $74,999'
                when q_demos_income = '$75,000 - $99,999'
                then '4 - $75,000 - $99,999'
                when q_demos_income = '$100,000 - $149,999'
                then '5 - $100,000 - $149,999'
                when q_demos_income = '$150,000 or more'
                then '6 - $150,000 or more'
                else '7 - Prefer not to say'
            end as demo_band,
            'income' as demo,
            count(*) as cell_counts
        from quintile_assignments
        group by 1, 2, 3, 4
        union all
        select
            recency_quintile as quintile,
            'recency' as metric,
            q_demos_gender as demo_band,
            'gender' as demo,
            count(*) as cell_counts
        from quintile_assignments
        group by 1, 2, 3, 4
        union all
        select
            recency_quintile as quintile,
            'recency' as metric,
            case when q_demos_hispanic = true then 'yes' else 'no' end as demo_band,
            'hispanic' as demo,
            count(*) as cell_counts
        from quintile_assignments
        group by 1, 2, 3, 4
    ),
    frequency_analysis as (
        select
            frequency_quintile as quintile,
            'frequency' as metric,
            q_demos_age as demo_band,
            'age' as demo,
            count(*) as cell_counts
        from quintile_assignments
        group by 1, 2, 3, 4
        union all
        select
            frequency_quintile as quintile,
            'frequency' as metric,
            case
                when q_demos_race = 'American Indian/Native American or Alaska Native'
                then 'American Indian/Native American or Alaska Native Alone'
                when q_demos_race = 'Asian'
                then 'Asian Alone'
                when q_demos_race = 'Black or African American'
                then 'Black or African American Alone'
                when q_demos_race = 'Native Hawaiian or Other Pacific Islander'
                then 'Native Hawaiian or Other Pacific Islander Alone'
                when q_demos_race = 'Other'
                then 'Other'
                when q_demos_race = 'White or Caucasian'
                then 'White or Caucasian Alone'
                else 'Multiracial'
            end as demo_band,
            'race' as demo,
            count(*) as cell_counts
        from quintile_assignments
        group by 1, 2, 3, 4
        union all
        select
            frequency_quintile as quintile,
            'frequency' as metric,
            case
                when q_demos_education = 'Some high school or less'
                then '1 - Some high school or less'
                when q_demos_education = 'High school diploma or GED'
                then '2 - High school diploma or GED'
                when q_demos_education = 'Prefer not to say'
                then '5 - Prefer not to say'
                when
                    q_demos_education
                    = 'Graduate or professional degree (MA, MS, MBA, PhD, JD, MD, DDS, etc)'
                then
                    '4 - Graduate or professional degree (MA, MS, MBA, PhD, JD, MD, DDS, etc)'
                else '3 - Bachelors degree'
            end as demo_band,
            'education' as demo,
            count(*) as cell_counts
        from quintile_assignments
        group by 1, 2, 3, 4
        union all
        select
            frequency_quintile as quintile,
            'frequency' as metric,
            case
                when q_demos_income = 'Less than $25,000'
                then '1 - Less than $25,000'
                when q_demos_income = '$25,000 - $49,999'
                then '2 - $25,000 - $49,999'
                when q_demos_income = '$50,000 - $74,999'
                then '3 - $50,000 - $74,999'
                when q_demos_income = '$75,000 - $99,999'
                then '4 - $75,000 - $99,999'
                when q_demos_income = '$100,000 - $149,999'
                then '5 - $100,000 - $149,999'
                when q_demos_income = '$150,000 or more'
                then '6 - $150,000 or more'
                else '7 - Prefer not to say'
            end as demo_band,
            'income' as demo,
            count(*) as cell_counts
        from quintile_assignments
        group by 1, 2, 3, 4
        union all
        select
            frequency_quintile as quintile,
            'frequency' as metric,
            q_demos_gender as demo_band,
            'gender' as demo,
            count(*) as cell_counts
        from quintile_assignments
        group by 1, 2, 3, 4
        union all
        select
            frequency_quintile as quintile,
            'frequency' as metric,
            case when q_demos_hispanic = true then 'yes' else 'no' end as demo_band,
            'hispanic' as demo,
            count(*) as cell_counts
        from quintile_assignments
        group by 1, 2, 3, 4
    ),
    monetary_analysis as (
        select
            monetary_quintile as quintile,
            'monetary' as metric,
            q_demos_age as demo_band,
            'age' as demo,
            count(*) as cell_counts
        from quintile_assignments
        group by 1, 2, 3, 4
        union all
        select
            monetary_quintile as quintile,
            'monetary' as metric,
            case
                when q_demos_race = 'American Indian/Native American or Alaska Native'
                then 'American Indian/Native American or Alaska Native Alone'
                when q_demos_race = 'Asian'
                then 'Asian Alone'
                when q_demos_race = 'Black or African American'
                then 'Black or African American Alone'
                when q_demos_race = 'Native Hawaiian or Other Pacific Islander'
                then 'Native Hawaiian or Other Pacific Islander Alone'
                when q_demos_race = 'Other'
                then 'Other'
                when q_demos_race = 'White or Caucasian'
                then 'White or Caucasian Alone'
                else 'Multiracial'
            end as demo_band,
            'race' as demo,
            count(*) as cell_counts
        from quintile_assignments
        group by 1, 2, 3, 4
        union all
        select
            monetary_quintile as quintile,
            'monetary' as metric,
            case
                when q_demos_education = 'Some high school or less'
                then '1 - Some high school or less'
                when q_demos_education = 'High school diploma or GED'
                then '2 - High school diploma or GED'
                when q_demos_education = 'Prefer not to say'
                then '5 - Prefer not to say'
                when
                    q_demos_education
                    = 'Graduate or professional degree (MA, MS, MBA, PhD, JD, MD, DDS, etc)'
                then
                    '4 - Graduate or professional degree (MA, MS, MBA, PhD, JD, MD, DDS, etc)'
                else '3 - Bachelors degree'
            end as demo_band,
            'education' as demo,
            count(*) as cell_counts
        from quintile_assignments
        group by 1, 2, 3, 4
        union all
        select
            monetary_quintile as quintile,
            'monetary' as metric,
            case
                when q_demos_income = 'Less than $25,000'
                then '1 - Less than $25,000'
                when q_demos_income = '$25,000 - $49,999'
                then '2 - $25,000 - $49,999'
                when q_demos_income = '$50,000 - $74,999'
                then '3 - $50,000 - $74,999'
                when q_demos_income = '$75,000 - $99,999'
                then '4 - $75,000 - $99,999'
                when q_demos_income = '$100,000 - $149,999'
                then '5 - $100,000 - $149,999'
                when q_demos_income = '$150,000 or more'
                then '6 - $150,000 or more'
                else '7 - Prefer not to say'
            end as demo_band,
            'income' as demo,
            count(*) as cell_counts
        from quintile_assignments
        group by 1, 2, 3, 4
        union all
        select
            monetary_quintile as quintile,
            'monetary' as metric,
            q_demos_gender as demo_band,
            'gender' as demo,
            count(*) as cell_counts
        from quintile_assignments
        group by 1, 2, 3, 4
        union all
        select
            monetary_quintile as quintile,
            'monetary' as metric,
            case when q_demos_hispanic = true then 'yes' else 'no' end as demo_band,
            'hispanic' as demo,
            count(*) as cell_counts
        from quintile_assignments
        group by 1, 2, 3, 4
    ),

    stack as (
        select *
        from recency_analysis
        union all
        select *
        from frequency_analysis
        union all
        select *
        from monetary_analysis
    ),
    row_total as (
        select q_demos_age as demo_band, 'age' as demo, count(*) as row_sum
        from quintile_assignments
        group by 1, 2
        union all
        select
            case
                when q_demos_race = 'American Indian/Native American or Alaska Native'
                then 'American Indian/Native American or Alaska Native Alone'
                when q_demos_race = 'Asian'
                then 'Asian Alone'
                when q_demos_race = 'Black or African American'
                then 'Black or African American Alone'
                when q_demos_race = 'Native Hawaiian or Other Pacific Islander'
                then 'Native Hawaiian or Other Pacific Islander Alone'
                when q_demos_race = 'Other'
                then 'Other'
                when q_demos_race = 'White or Caucasian'
                then 'White or Caucasian Alone'
                else 'Multiracial'
            end as demo_band,
            'race' as demo,
            count(*) as row_sum
        from quintile_assignments
        group by 1, 2
        union all
        select
            case
                when q_demos_education = 'Some high school or less'
                then '1 - Some high school or less'
                when q_demos_education = 'High school diploma or GED'
                then '2 - High school diploma or GED'
                when q_demos_education = 'Prefer not to say'
                then '5 - Prefer not to say'
                when
                    q_demos_education
                    = 'Graduate or professional degree (MA, MS, MBA, PhD, JD, MD, DDS, etc)'
                then
                    '4 - Graduate or professional degree (MA, MS, MBA, PhD, JD, MD, DDS, etc)'
                else '3 - Bachelors degree'
            end as demo_band,
            'education' as demo,
            count(*) as row_sum
        from quintile_assignments
        group by 1, 2
        union all
        select
            case
                when q_demos_income = 'Less than $25,000'
                then '1 - Less than $25,000'
                when q_demos_income = '$25,000 - $49,999'
                then '2 - $25,000 - $49,999'
                when q_demos_income = '$50,000 - $74,999'
                then '3 - $50,000 - $74,999'
                when q_demos_income = '$75,000 - $99,999'
                then '4 - $75,000 - $99,999'
                when q_demos_income = '$100,000 - $149,999'
                then '5 - $100,000 - $149,999'
                when q_demos_income = '$150,000 or more'
                then '6 - $150,000 or more'
                else '7 - Prefer not to say'
            end as demo_band,
            'income' as demo,
            count(*) as row_sum
        from quintile_assignments
        group by 1, 2
        union all
        select q_demos_gender as demo_band, 'gender' as demo, count(*) as row_sum
        from quintile_assignments
        group by 1, 2
        union all
        select
            case when q_demos_hispanic = true then 'yes' else 'no' end as demo_band,
            'hispanic' as demo,
            count(*) as row_sum
        from quintile_assignments
        group by 1, 2
    ),
    column_total as (
        select recency_quintile as quintile, 'recency' as metric, count(*) as column_sum
        from quintile_assignments
        group by 1, 2
        union all
        select
            frequency_quintile as quintile,
            'frequency' as metric,
            count(*) as column_sum
        from quintile_assignments
        group by 1, 2
        union all
        select
            monetary_quintile as quintile, 'monetary' as metric, count(*) as column_sum
        from quintile_assignments
        group by 1, 2
    ),
    overall as (
        select count(*) as overall_total from `dbt_dreynolds.fct_user_purchase_metrics`
    )
select s.*, r.row_sum, c.column_sum, o.overall_total
from stack s
left join row_total r on r.demo = s.demo and r.demo_band = s.demo_band
left join column_total c on c.quintile = s.quintile and c.metric = s.metric
cross join overall o
