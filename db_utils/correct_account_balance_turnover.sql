CREATE OR REPLACE PROCEDURE dm.correct_account_balance_turnover()
AS $$
DECLARE
	start_time timestamp := clock_timestamp();
	end_time timestamp;
	duration int;
BEGIN

	WITH current_day AS (
		SELECT account_rk, effective_date, account_out_sum
		FROM dm.account_balance_turnover
	),
	previous_day as(
		SELECT account_rk, effective_date + INTERVAL '1 day' AS effective_date, account_out_sum
		FROM dm.account_balance_turnover
	),
	correct_balances as(
	SELECT c.account_rk,
			c.effective_date,
			p.account_out_sum AS account_in_sum,
			c.account_out_sum
	FROM current_day c
	JOIN previous_day p
	ON c.account_rk = p.account_rk
		AND c.effective_date = p.effective_date
	)
	
	update dm.account_balance_turnover ab
	set account_in_sum = cb.account_in_sum
	from correct_balances cb
	where ab.account_rk = cb.account_rk
		and ab.effective_date = cb.effective_date;
	
	end_time := clock_timestamp();
	duration := extract(epoch from (end_time - start_time))::INT;

	insert into logs.etl_log(operation_name,
							start_time,
							end_time,
							duration_sec)
	values('correct_account_balance_turnover', start_time, end_time, duration);
	
	exception
		when others then
			raise notice 'correct_account_balance_turnover error: %', sqlerrm;
END;
$$ LANGUAGE plpgsql;


CALL dm.correct_account_balance_turnover();

