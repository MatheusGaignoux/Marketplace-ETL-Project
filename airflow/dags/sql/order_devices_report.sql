create view order_devices_report
as
select "Device",
	count(distinct "OrderId") as qttOrders
from order_devices
group by "Device"
order by 2 desc, 1 asc
limit 5
;