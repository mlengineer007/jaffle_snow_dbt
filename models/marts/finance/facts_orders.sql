{{
    config(materialized = 'view')
}}


with p as ( select 
order_id ,
status,
amount
 from 
{{ ref("stg_stripe__payments")}} 
),

p_success as (
  select order_id,
  sum(case when status='success' then amount end) as amount
  from p 
  group by 1

),

 o as (
    select order_id,  customer_id , order_date from {{ref("stg_orders")}} 
),


final as ( select 
    p_success.order_id as order_id,
    o.customer_id as customer_id,
    o.order_date,



    coalesce(p_success.amount,0) as amount
from p_success left  join o on  p_success.order_id = o.order_id

)

select * from final