with adv_status_cte as (
SELECT COALESCE(a.user_id,d.user_id) as user_id,a.status as status,
case when d.user_id = a.user_id then 'Paid' else 'Not Paid' end as payment_status
FROM advertiser a 
full join 
daily_pay d on a.user_id = d.user_id 
)
select user_id,
case 
when 
(status in ('NEW','EXISTING','RESURRECT')  and payment_status in ('Paid') ) 
then 'EXISTING'
when 
(status in ('CHURN')  and payment_status in ('Paid') ) 
then 'RESURRECT'
when 
(status in ('NEW','EXISTING','CHURN','RESURRECT')  and payment_status in ('Not Paid') )
then 'CHURN'
else 'NEW'
end as new_status
from 
adv_status_cte order by 1

