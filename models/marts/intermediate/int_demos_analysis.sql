with
    purchases_responses as (select * from {{ ref("int_purchases_responses") }}),

    demos as (
        select
            q_demos_age as demo_band,
            category,
            'age' as metric,
            count(distinct survey_responseid) as users,
            count(*) as purchases,
            sum(total_spend) as total_spend
        from purchases_responses
        group by 1, 2, 3
        union all
        select
            case when q_demos_hispanic = true then 'yes' else 'no' end as demo_band,
            category,
            'hispanic' as metric,
            count(distinct survey_responseid) as users,
            count(*) as purchases,
            sum(total_spend) as total_spend
        from purchases_responses
        group by 1, 2, 3
        union all
        select
            q_demos_race as demo_band,
            category,
            'race' as metric,
            count(distinct survey_responseid) as users,
            count(*) as purchases,
            sum(total_spend) as total_spend
        from purchases_responses
        group by 1, 2, 3
        union all
        select
            q_demos_education as demo_band,
            category,
            'education' as metric,
            count(distinct survey_responseid) as users,
            count(*) as purchases,
            sum(total_spend) as total_spend
        from purchases_responses
        group by 1, 2, 3
        union all
        select
            q_demos_income as demo_band,
            category,
            'income' as metric,
            count(distinct survey_responseid) as users,
            count(*) as purchases,
            sum(total_spend) as total_spend
        from purchases_responses
        group by 1, 2, 3
        union all
        select
            q_demos_gender as demo_band,
            category,
            'gender' as metric,
            count(distinct survey_responseid) as users,
            count(*) as purchases,
            sum(total_spend) as total_spend
        from purchases_responses
        group by 1, 2, 3
        union all
        select
            q_demos_state as demo_band,
            category,
            'state' as metric,
            count(distinct survey_responseid) as users,
            count(*) as purchases,
            sum(total_spend) as total_spend
        from purchases_responses
        group by 1, 2, 3
    )

select *
from demos
