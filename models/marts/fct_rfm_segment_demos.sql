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
    rfm_segments as (
        select
            *,
            case
                when
                    (frequency_quintile = 1 and recency_quintile = 1)
                    or (frequency_quintile = 1 and recency_quintile = 2)
                    or (frequency_quintile = 2 and recency_quintile = 1)
                    or (frequency_quintile = 2 and recency_quintile = 2)
                then 'Hibernating'
                when
                    (frequency_quintile = 3 and recency_quintile = 1)
                    or (frequency_quintile = 3 and recency_quintile = 2)
                    or (frequency_quintile = 4 and recency_quintile = 1)
                    or (frequency_quintile = 4 and recency_quintile = 2)
                then 'At Risk'
                when
                    (frequency_quintile = 5 and recency_quintile = 1)
                    or (frequency_quintile = 5 and recency_quintile = 2)
                then 'Cannot Lose Them'
                when
                    (frequency_quintile = 1 and recency_quintile = 3)
                    or (frequency_quintile = 2 and recency_quintile = 3)
                then 'About to Sleep'
                when (frequency_quintile = 3 and recency_quintile = 3)
                then 'Need Attention'
                when
                    (frequency_quintile = 4 and recency_quintile = 3)
                    or (frequency_quintile = 4 and recency_quintile = 4)
                    or (frequency_quintile = 5 and recency_quintile = 3)
                    or (frequency_quintile = 5 and recency_quintile = 4)
                then 'Loyal Customers'
                when (frequency_quintile = 1 and recency_quintile = 4)
                then 'Promising'
                when
                    (frequency_quintile = 2 and recency_quintile = 4)
                    or (frequency_quintile = 2 and recency_quintile = 5)
                    or (frequency_quintile = 3 and recency_quintile = 4)
                    or (frequency_quintile = 3 and recency_quintile = 5)
                then 'Potential Loyalists'
                when
                    (frequency_quintile = 4 and recency_quintile = 5)
                    or (frequency_quintile = 5 and recency_quintile = 5)
                then 'Champions'
                when (frequency_quintile = 1 and recency_quintile = 5)
                then 'New Customers'
                else 'Remaining'
            end as rfm_segment
        from quintile_assignments
    ),
    analysis_counts as (
        select
            rfm_segment,
            q_demos_age as demo_band,
            'age' as demo,
            count(*) as cell_counts
        from rfm_segments
        group by 1, 2, 3
        union all
        select
            rfm_segment,
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
        from rfm_segments
        group by 1, 2, 3
        union all
        select
            rfm_segment,
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
                then '4 - Graduate or professional degree'
                else '3 - Bachelors degree'
            end as demo_band,
            'education' as demo,
            count(*) as cell_counts
        from rfm_segments
        group by 1, 2, 3
        union all
        select
            rfm_segment,
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
        from rfm_segments
        group by 1, 2, 3
        union all
        select
            rfm_segment,
            q_demos_gender as demo_band,
            'gender' as demo,
            count(*) as cell_counts
        from rfm_segments
        group by 1, 2, 3
        union all
        select
            rfm_segment,
            case when q_demos_hispanic = true then 'yes' else 'no' end as demo_band,
            'hispanic' as demo,
            count(*) as cell_counts
        from rfm_segments
        group by 1, 2, 3
    ),
    row_total as (
        select q_demos_age as demo_band, 'age' as demo, count(*) as row_sum
        from rfm_segments
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
        from rfm_segments
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
        from rfm_segments
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
        from rfm_segments
        group by 1, 2
        union all
        select q_demos_gender as demo_band, 'gender' as demo, count(*) as row_sum
        from rfm_segments
        group by 1, 2
        union all
        select
            case when q_demos_hispanic = true then 'yes' else 'no' end as demo_band,
            'hispanic' as demo,
            count(*) as row_sum
        from rfm_segments
        group by 1, 2
    ),
    column_total as (
        select rfm_segment, count(*) as column_sum, sum(total_spend) as total_spend
        from rfm_segments
        group by 1
    ),
    overall as (
        select count(*) as overall_total from `dbt_dreynolds.fct_user_purchase_metrics`
    )
select a.*, r.row_sum, c.column_sum, c.total_spend, o.overall_total
from analysis_counts a
left join row_total r on r.demo = a.demo and r.demo_band = a.demo_band
left join column_total c on c.rfm_segment = a.rfm_segment
cross join overall o
