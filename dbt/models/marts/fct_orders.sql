select
  message_id,
  platform,
  account_email,
  label,
  order_ts,
  date_trunc('day', order_ts) as order_date,
  coalesce(merchant, 'Unknown') as merchant,
  subtotal,
  fees,
  total,
  currency
from {{ ref('stg_orders') }}
where order_ts is not null