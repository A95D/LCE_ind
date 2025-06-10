

select
	client_id,
	client_name,
	client_revenue,
	usage_count,
	case
		usage_count
		when 1 then '1 продукт'
		when 2 then '2 продукта'
		else '3+ продукта'
	end client_segment,
	last_usage_date
from 
	(
	select
		c.id client_id,
		c.name client_name,
		c.revenue client_revenue,
		max(p.usage_date) last_usage_date,
		count(p.id) usage_count
	from
		staging.clients c
	join staging.product_usage p on
		p.client_id = c.id
	group by
		1
	order by
		2
	) usage_by_clients;