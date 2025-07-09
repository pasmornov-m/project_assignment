CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f (i_OnDate DATE)
AS $$
DECLARE
    start_time TIMESTAMP := clock_timestamp();
    end_time TIMESTAMP;
    duration INT;
BEGIN
	DELETE 
	FROM dm.dm_account_turnover_f
	WHERE on_date = i_OnDate;
	
	INSERT INTO dm.dm_account_turnover_f(	on_date,
											account_rk,
											credit_amount,
											credit_amount_rub,
											debet_amount,
											debet_amount_rub)
	with input_date as(
	select i_OnDate::date as i_OnDate
	),
	sub_t as(
	select credit_account_rk, debet_account_rk, credit_amount, debet_amount
	from ds.ft_posting_f, input_date
	where oper_date = input_date.i_OnDate
	),
	all_accounts as(
		select credit_account_rk as account_rk, credit_amount, 0::numeric as debet_amount
		from sub_t
		union all
		select debet_account_rk as account_rk, 0::numeric as credit_amount, debet_amount
		from sub_t
	),
	all_amounts as(
		select account_rk, sum(credit_amount) as credit_amount, sum(debet_amount) as debet_amount
		from all_accounts
		group by account_rk
	),
	account_currency AS (
	    SELECT account_rk, currency_rk
	    FROM ds.md_account_d, input_date
	    WHERE input_date.i_OnDate BETWEEN data_actual_date AND data_actual_end_date
	),
	ex_rate as(
		SELECT reduced_cource, currency_rk
		FROM ds.md_exchange_rate_d, input_date
		WHERE input_date.i_OnDate BETWEEN data_actual_date AND data_actual_end_date
	),
	joined_table as(
		select aa.account_rk, 
		credit_amount, coalesce(reduced_cource,1) * credit_amount as credit_amount_rub, 
		debet_amount, coalesce(reduced_cource,1) * debet_amount as debet_amount_rub
		from
		all_amounts aa
		left join
		account_currency ac
		on aa.account_rk = ac.account_rk
		left join
		ex_rate er
		on ac.currency_rk = er.currency_rk
	)
	select input_date.i_OnDate, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub
	from joined_table, input_date;

	end_time := clock_timestamp();
	duration := extract(epoch from (end_time - start_time))::INT;

	insert into logs.etl_log(operation_name,
							start_time,
							end_time,
							duration_sec)
	values('fill_account_turnover_f('||i_OnDate||')', start_time, end_time, duration);
	
	exception
	    when others then
	        raise notice 'fill_account_turnover_f error: %', sqlerrm;
END;
$$ LANGUAGE plpgsql;


do $$
declare
	d date := date '2018-01-01';
begin
	while d <= '2018-01-31' loop
		call ds.fill_account_turnover_f(d);
		d := d + interval '1 day';
	end loop;
end $$;

