DELETE 
FROM dm.dm_account_balance_f
WHERE on_date = '2017-12-31';

INSERT INTO dm.dm_account_balance_f(on_date, account_rk, balance_out, balance_out_rub)
SELECT on_date, account_rk, balance_out, coalesce(reduced_cource,1) * balance_out AS balance_out_rub
FROM ds.ft_balance_f
LEFT JOIN 
(SELECT reduced_cource, currency_rk
FROM ds.md_exchange_rate_d
WHERE '2017-12-31' BETWEEN data_actual_date AND data_actual_end_date) t1
USING(currency_rk)
WHERE on_date = '2017-12-31'



CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
AS $$
DECLARE
    start_time TIMESTAMP := clock_timestamp();
    end_time TIMESTAMP;
    duration INT;
BEGIN
	DELETE 
	FROM dm.dm_account_balance_f
	WHERE on_date = i_OnDate;

	INSERT INTO dm.dm_account_balance_f(on_date, account_rk, balance_out, balance_out_rub)
	with input_date as (
		select i_OnDate::date as i_OnDate
	),
	accounts as (
		select account_rk, char_type, currency_rk
		from ds.md_account_d, input_date
		where input_date.i_OnDate between data_actual_date and data_actual_end_date
	),
	prev_values as (
		select account_rk, balance_out, balance_out_rub
		from dm.dm_account_balance_f, input_date
		where on_date = (input_date.i_OnDate - interval '1 day')::date
	),
	prev_balances as (
		select 
		acc.account_rk, 
		char_type, 
		coalesce(pv.balance_out,0) as prev_balance,
		coalesce(pv.balance_out_rub,0) as prev_balance_rub,
		dat.credit_amount , 
		dat.credit_amount_rub , 
		dat.debet_amount , 
		dat.debet_amount_rub 
		from accounts acc
		cross join input_date
		left join prev_values pv
		on acc.account_rk = pv.account_rk
		left join dm.dm_account_turnover_f dat
		on acc.account_rk = dat.account_rk and dat.on_date = input_date.i_OnDate
	),
	current_balances as (
		select
		input_date.i_OnDate,
		account_rk,
		case
			when char_type = 'А' then coalesce(prev_balance,0) + coalesce(debet_amount,0) - coalesce(credit_amount,0)
			when char_type = 'П' then coalesce(prev_balance,0) - coalesce(debet_amount,0) + coalesce(credit_amount,0)
		end as balance_out,
		case
			when char_type = 'А' then coalesce(prev_balance_rub,0) + coalesce(debet_amount_rub,0) - coalesce(credit_amount_rub,0)
			when char_type = 'П' then coalesce(prev_balance_rub,0) - coalesce(debet_amount_rub,0) + coalesce(credit_amount_rub,0)
		end as balance_out_rub
		from prev_balances, input_date
	)
	select cb.i_OnDate, account_rk, balance_out, balance_out_rub
	from current_balances cb;
	
	
	end_time := clock_timestamp();
	duration := extract(epoch from (end_time - start_time))::INT;

	insert into logs.etl_log(operation_name,
							start_time,
							end_time,
							duration_sec)
	values('fill_account_balance_f('||i_OnDate||')', start_time, end_time, duration);
	
	exception
	    when others then
	        raise notice 'fill_account_balance_f error: %', sqlerrm;
END;
$$ LANGUAGE plpgsql;



do $$
declare
	d date := date '2018-01-01';
begin
	while d <= '2018-01-31' loop
		call ds.fill_account_balance_f(d);
		d := d + interval '1 day';
	end loop;
end $$;


	