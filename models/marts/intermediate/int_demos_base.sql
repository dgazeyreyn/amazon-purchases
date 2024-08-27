with
    purchases_responses as (select * from {{ ref("int_purchases_responses") }}),

    demos as (
        select
            q_demos_age as demo_band,
            'age' as metric,
            count(distinct survey_responseid) as users
        from purchases_responses
        group by 1, 2
        union all
        select
            case when q_demos_hispanic = true then 'yes' else 'no' end as demo_band,
            'hispanic' as metric,
            count(distinct survey_responseid) as users
        from purchases_responses
        group by 1, 2
        union all
        select
            q_demos_race as demo_band,
            'race' as metric,
            count(distinct survey_responseid) as users
        from purchases_responses
        group by 1, 2
        union all
        select
            q_demos_education as demo_band,
            'education' as metric,
            count(distinct survey_responseid) as users
        from purchases_responses
        group by 1, 2
        union all
        select
            q_demos_income as demo_band,
            'income' as metric,
            count(distinct survey_responseid) as users
        from purchases_responses
        group by 1, 2
        union all
        select
            q_demos_gender as demo_band,
            'gender' as metric,
            count(distinct survey_responseid) as users
        from purchases_responses
        group by 1, 2
        union all
        select
            q_demos_state as demo_band,
            'state' as metric,
            count(distinct survey_responseid) as users
        from purchases_responses
        group by 1, 2
    )

select *
from demos