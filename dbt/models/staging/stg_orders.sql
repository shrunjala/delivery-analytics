select
  message_id,
  platform,
  account_email,
  label,
  order_ts,
  merchant,
  subtotal,
  fees,
  total,
  currency,
  raw_subject
from {{ source('raw','raw_orders') }}
