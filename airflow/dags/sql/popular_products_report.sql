create view popular_products_report
as
select distinct "Country",
	"ProductName",
	count(distinct "HostIp") as qttLogs
from popular_products
where 1 = 1
	and "Country" in (
		select "Country"
		from popular_products
		group by "Country"
		order by count(distinct "HostIp") desc
		limit 1
	)
group by 1, 2
order by 3 desc
;
