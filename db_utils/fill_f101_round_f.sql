CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f (i_OnDate DATE)
AS $$
DECLARE
    start_time TIMESTAMP := clock_timestamp();
    end_time TIMESTAMP;
    duration INT;
BEGIN
	DELETE 
	FROM dm.dm_f101_round_f
	WHERE i_OnDate = (to_date + interval '1 day')::DATE;

	INSERT INTO dm.dm_f101_round_f(from_date,
									to_date,
									chapter,
									ledger_account,
									characteristic,
									balance_in_rub,
									balance_in_val,
									balance_in_total,
									turn_deb_rub,
									turn_deb_val,
									turn_deb_total,
									turn_cre_rub,
									turn_cre_val,
									turn_cre_total,
									balance_out_rub,
									balance_out_val,
									balance_out_total)

	WITH input_date AS (
		SELECT i_OnDate::DATE AS i_OnDate
	),
	period AS (
		SELECT 
		(input_date.i_OnDate - INTERVAL '1 month')::DATE AS from_date,
		(input_date.i_OnDate - INTERVAL '1 day')::DATE AS to_date
		FROM input_date
	),
	ledger_t as (
		SELECT chapter, ledger_account
		from ds.md_ledger_account_s
	),
	accounts as (
		select 
		from_date, 
		to_date,
		account_rk,
		left(account_number, 5)::int as ledger_account,
		char_type as characteristic,
		currency_code
		from ds.md_account_d, period
		where from_date between data_actual_date and data_actual_end_date 
		and to_date between data_actual_date and data_actual_end_date
	),
	in_balances as (
		select
		account_rk,
		sum(case 
			when currency_code in ('643', '810') then balance_out_rub
			else 0
		end) as balance_in_rub,
		sum(case 
			when currency_code not in ('643', '810') then balance_out_rub
			else 0
		end) as balance_in_val,
		sum(balance_out_rub) as balance_in_total
		from 
		dm.dm_account_balance_f
		join
		accounts
		using(account_rk)
		where on_date = from_date - interval '1 day'
		group by account_rk
	),
	deb_balances as (
		select account_rk,
		sum(case 
			when currency_code in ('643', '810') then debet_amount_rub
			else 0
		end) as turn_deb_rub,
		sum(case 
			when currency_code not in ('643', '810') then debet_amount_rub
			else 0
		end) as turn_deb_val,
		sum(debet_amount_rub) as turn_deb_total
		from 
		dm.dm_account_turnover_f
		join
		accounts
		using(account_rk)
		where on_date between from_date and to_date
		group by account_rk
	),
	cre_balances as (
		select account_rk,
		sum(case 
			when currency_code in ('643', '810') then credit_amount_rub
			else 0
		end) as turn_cre_rub,
		sum(case 
			when currency_code not in ('643', '810') then credit_amount_rub
			else 0
		end) as turn_cre_val,
		sum(credit_amount_rub) as turn_cre_total
		from 
		dm.dm_account_turnover_f
		join
		accounts
		using(account_rk)
		where on_date between from_date and to_date
		group by account_rk
	),
	out_balances as (
		select
		account_rk,
		sum(case 
			when currency_code in ('643', '810') then balance_out_rub
			else 0
		end) as balance_out_rub,
		sum(case 
			when currency_code not in ('643', '810') then balance_out_rub
			else 0
		end) as balance_out_val,
		sum(balance_out_rub) as balance_out_total
		from 
		dm.dm_account_balance_f
		join
		accounts
		using(account_rk)
		where on_date = to_date
		group by account_rk
	),
	prefinal_balances as (
		select
		ledger_account,
		characteristic,
		sum(coalesce(balance_in_rub,0)) as balance_in_rub,
		sum(coalesce(balance_in_val,0)) as balance_in_val,
		sum(coalesce(balance_in_total,0)) as balance_in_total,
		sum(coalesce(turn_deb_rub,0)) as turn_deb_rub,
		sum(coalesce(turn_deb_val,0)) as turn_deb_val,
		sum(coalesce(turn_deb_total,0)) as turn_deb_total,
		sum(coalesce(turn_cre_rub,0)) as turn_cre_rub,
		sum(coalesce(turn_cre_val,0)) as turn_cre_val,
		sum(coalesce(turn_cre_total,0)) as turn_cre_total,
		sum(coalesce(balance_out_rub,0)) as balance_out_rub,
		sum(coalesce(balance_out_val,0)) as balance_out_val,
		sum(coalesce(balance_out_total,0)) as balance_out_total
		from
		accounts
		left join
		in_balances
		using(account_rk)
		left join
		deb_balances
		using(account_rk)
		left join
		cre_balances 
		using(account_rk)
		left join
		out_balances
		using(account_rk)
		group by ledger_account, characteristic
	),
	all_balances as (
		select distinct
		from_date,
		to_date,
		chapter,
		ledger_account,
		prefinal_balances.characteristic,
		balance_in_rub,
		balance_in_val,
		balance_in_total,
		turn_deb_rub,
		turn_deb_val,
		turn_deb_total,
		turn_cre_rub,
		turn_cre_val,
		turn_cre_total,
		balance_out_rub,
		balance_out_val,
		balance_out_total
		from
		prefinal_balances
		join ledger_t
		using(ledger_account)
		cross join period
	)
	select
	from_date,
	to_date,
	chapter,
	ledger_account,
	characteristic,
	balance_in_rub,
	balance_in_val,
	balance_in_total,
	turn_deb_rub,
	turn_deb_val,
	turn_deb_total,
	turn_cre_rub,
	turn_cre_val,
	turn_cre_total,
	balance_out_rub,
	balance_out_val,
	balance_out_total
	FROM all_balances;
	
	
	end_time := clock_timestamp();
	duration := extract(epoch from (end_time - start_time))::INT;

	insert into logs.etl_log(operation_name,
							start_time,
							end_time,
							duration_sec)
	values('fill_f101_round_f('||i_OnDate||')', start_time, end_time, duration);
	
	exception
	    when others then
	        raise notice 'fill_f101_round_f error: %', sqlerrm;
END;
$$ LANGUAGE plpgsql;



call dm.fill_f101_round_f('2018-02-01');

