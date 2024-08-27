{{ config(materialized="table") }}
with
    analysis as (select * from {{ ref("int_demos_analysis") }}),

    base as (select * from {{ ref("int_demos_base") }}),

    final as (
        select
            analysis.*,
            base.users as base_users,
            base.purchases as base_purchases,
            base.total_spend as base_total_spend
        from analysis
        left join
            base
            on base.demo_band = analysis.demo_band
            and base.metric = analysis.metric
    )

select *
from final
