create or replace procedure dm.client_drop_duplicates()
as $$
declare
	start_time timestamp := clock_timestamp();
	end_time timestamp;
	duration int;
begin
	delete 
	from dm.client
	where ctid in (
	    select ctid
	    from (
	        select ctid,
	               row_number() over (
	                   partition by client_rk, effective_from_date
	                   order by ctid
	               ) as rn
	        from dm.client
	    ) t
	    where rn > 1
	);
end;
$$ language plpgsql;


call dm.client_drop_duplicates();

