create view last_year_orders_report
as
select max(to_char("orderDate", 'YYYY')) as lastYear, 
    to_char("orderDate", 'MM') as month, 
    count(distinct "orderId") as qttOrders
from last_year_orders
group by to_char("orderDate", 'MM')
order by to_char("orderDate", 'MM') asc
;