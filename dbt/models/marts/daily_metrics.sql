with base as (
  select
    order_date,
    platform,
    count(*) as orders,
    sum(total) as spend,
    avg(total) as aov,
    sum(coalesce(fees, 0)) as fees,
    case when sum(total) > 0 then sum(coalesce(fees,0))/sum(total) else null end as fees_pct
  from {{ ref('fct_orders') }}
  group by 1,2
),
roll as (
  select
    *,
    sum(spend) over (partition by platform order by order_date rows between 6 preceding and current row) as spend_7d,
    sum(spend) over (partition by platform order by order_date rows between 29 preceding and current row) as spend_30d
  from base
)
select * from roll
