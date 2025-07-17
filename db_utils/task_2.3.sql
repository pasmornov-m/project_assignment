
-- 1
--Подготовить запрос, который определит корректное значение поля account_in_sum. 
--Если значения полей account_in_sum одного дня и account_out_sum предыдущего дня отличаются, 
--то корректным выбирается значение account_out_sum предыдущего дня.

WITH current_day AS (
	SELECT account_rk, effective_date, account_out_sum
	FROM dm.account_balance_turnover
),
previous_day as(
	SELECT account_rk, effective_date + INTERVAL '1 day' AS effective_date, account_out_sum
	FROM dm.account_balance_turnover
)
SELECT c.account_rk,
		c.effective_date,
		p.account_out_sum AS account_in_sum,
		c.account_out_sum
FROM current_day c
JOIN previous_day p
ON c.account_rk = p.account_rk
	AND c.effective_date = p.effective_date
		

-- 2
--Подготовить такой же запрос, только проблема теперь в том, 
--что account_in_sum одного дня правильная, а account_out_sum предыдущего дня некорректна. 
--Это означает, что если эти значения отличаются, то корректным значением для account_out_sum 
--предыдущего дня выбирается значение account_in_sum текущего дня.

WITH current_day AS (
	SELECT account_rk, effective_date - INTERVAL '1 day' AS effective_date, account_in_sum
	FROM dm.account_balance_turnover
),
previous_day as(
	SELECT account_rk, effective_date, account_in_sum
	FROM dm.account_balance_turnover
)
SELECT p.account_rk,
		p.effective_date,
		c.account_in_sum AS account_out_sum,
		p.account_in_sum
FROM current_day c
JOIN previous_day p
ON c.account_rk = p.account_rk
	AND c.effective_date = p.effective_date
	
	
	
	