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
	
	end_time := clock_timestamp();
	duration := extract(epoch from (end_time - start_time))::INT;

	insert into logs.etl_log(operation_name,
							start_time,
							end_time,
							duration_sec)
	values('client_drop_duplicates', start_time, end_time, duration);
	
	exception
		when others then
			raise notice 'client_drop_duplicates error: %', sqlerrm;
end;
$$ language plpgsql;


call dm.client_drop_duplicates();

