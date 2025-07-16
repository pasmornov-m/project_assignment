create or replace procedure dm.fill_loan_holiday_info()
as $$
declare
	start_time timestamp := clock_timestamp();
	end_time timestamp;
	duration int;
begin
	with deal as (
		select  account_rk,
				deal_rk
			   ,deal_num --Номер сделки
			   ,deal_name --Наименование сделки
			   ,deal_sum --Сумма сделки
			   ,client_rk --Ссылка на клиента
			   ,agreement_rk --Ссылка на договор
			   ,deal_start_date --Дата начала действия сделки
			   ,department_rk --Ссылка на отделение
			   ,product_rk -- Ссылка на продукт
			   ,deal_type_cd
			   ,effective_from_date
			   ,effective_to_date
		from RD.deal_info
		), 
		loan_holiday as (
		select  deal_rk,
			   loan_holiday_type_cd,  --Ссылка на тип кредитных каникул
			   loan_holiday_start_date     --Дата начала кредитных каникул
			   ,loan_holiday_finish_date    --Дата окончания кредитных каникул
			   ,loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
			   ,loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
			   ,loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
			   ,effective_from_date
			   ,effective_to_date
		from RD.loan_holiday
		), 
		product as (
		select product_rk
			  ,product_name
			  ,effective_from_date
			  ,effective_to_date
		from RD.product
		), 
		holiday_info as (
		select   d.account_rk,
				 d.deal_rk,
		        lh.effective_from_date,
		        lh.effective_to_date
		        ,d.deal_num as deal_number --Номер сделки
			    ,lh.loan_holiday_type_cd  --Ссылка на тип кредитных каникул
		        ,lh.loan_holiday_start_date     --Дата начала кредитных каникул
		        ,lh.loan_holiday_finish_date    --Дата окончания кредитных каникул
		        ,lh.loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
		        ,lh.loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
		        ,lh.loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
		        ,d.deal_name --Наименование сделки
		        ,d.deal_sum --Сумма сделки
		        ,d.client_rk --Ссылка на контрагента
		        ,d.agreement_rk --Ссылка на договор
		        ,d.deal_start_date --Дата начала действия сделки
		        ,d.department_rk --Ссылка на ГО/филиал
		        ,d.product_rk -- Ссылка на продукт
		        ,p.product_name -- Наименование продукта
		        ,d.deal_type_cd -- Наименование типа сделки
		from deal d
		left join loan_holiday lh on d.deal_rk = lh.deal_rk
		                             and d.effective_from_date = lh.effective_from_date
		left join product p on p.product_rk = d.product_rk
		),
		final_loan_holiday_info as (
		SELECT deal_rk
		      ,effective_from_date
		      ,effective_to_date
		      ,agreement_rk
			  ,account_rk
		      ,client_rk
		      ,department_rk
		      ,product_rk
		      ,product_name
		      ,deal_type_cd
		      ,deal_start_date
		      ,deal_name
		      ,deal_number
		      ,deal_sum
		      ,loan_holiday_type_cd
		      ,loan_holiday_start_date
		      ,loan_holiday_finish_date
		      ,loan_holiday_fact_finish_date
		      ,loan_holiday_finish_flg
		      ,loan_holiday_last_possible_date
		FROM holiday_info
		)
		update dm.loan_holiday_info d
	    set
	        agreement_rk = coalesce(d.agreement_rk, f.agreement_rk),
			account_rk = coalesce(d.account_rk, f.account_rk),
	        client_rk = coalesce(d.client_rk, f.client_rk),
	        department_rk = coalesce(d.department_rk, f.department_rk),
	        product_rk = coalesce(d.product_rk, f.product_rk),
	        product_name = coalesce(d.product_name, f.product_name),
	        deal_type_cd = coalesce(d.deal_type_cd, f.deal_type_cd),
	        deal_start_date = coalesce(d.deal_start_date, f.deal_start_date),
	        deal_name = coalesce(d.deal_name, f.deal_name),
	        deal_number = coalesce(d.deal_number, f.deal_number),
	        deal_sum = coalesce(d.deal_sum, f.deal_sum),
	        loan_holiday_type_cd = coalesce(d.loan_holiday_type_cd, f.loan_holiday_type_cd),
	        loan_holiday_start_date = coalesce(d.loan_holiday_start_date, f.loan_holiday_start_date),
	        loan_holiday_finish_date = coalesce(d.loan_holiday_finish_date, f.loan_holiday_finish_date),
	        loan_holiday_fact_finish_date = coalesce(d.loan_holiday_fact_finish_date, f.loan_holiday_fact_finish_date),
	        loan_holiday_finish_flg = coalesce(d.loan_holiday_finish_flg, f.loan_holiday_finish_flg),
	        loan_holiday_last_possible_date = coalesce(d.loan_holiday_last_possible_date, f.loan_holiday_last_possible_date)
	    from final_loan_holiday_info f
	    where d.deal_rk = f.deal_rk
	      and d.effective_from_date = f.effective_from_date
	      and d.effective_to_date = f.effective_to_date;
		

		with deal as (
		select  account_rk,
				deal_rk
			   ,deal_num --Номер сделки
			   ,deal_name --Наименование сделки
			   ,deal_sum --Сумма сделки
			   ,client_rk --Ссылка на клиента
			   ,agreement_rk --Ссылка на договор
			   ,deal_start_date --Дата начала действия сделки
			   ,department_rk --Ссылка на отделение
			   ,product_rk -- Ссылка на продукт
			   ,deal_type_cd
			   ,effective_from_date
			   ,effective_to_date
		from RD.deal_info
		), 
		loan_holiday as (
		select  deal_rk,
			   loan_holiday_type_cd,  --Ссылка на тип кредитных каникул
			   loan_holiday_start_date     --Дата начала кредитных каникул
			   ,loan_holiday_finish_date    --Дата окончания кредитных каникул
			   ,loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
			   ,loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
			   ,loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
			   ,effective_from_date
			   ,effective_to_date
		from RD.loan_holiday
		), 
		product as (
		select product_rk
			  ,product_name
			  ,effective_from_date
			  ,effective_to_date
		from RD.product
		), 
		holiday_info as (
		select   d.account_rk,
				 d.deal_rk
		        ,lh.effective_from_date
		        ,lh.effective_to_date
		        ,d.deal_num as deal_number --Номер сделки
			    ,lh.loan_holiday_type_cd  --Ссылка на тип кредитных каникул
		        ,lh.loan_holiday_start_date     --Дата начала кредитных каникул
		        ,lh.loan_holiday_finish_date    --Дата окончания кредитных каникул
		        ,lh.loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
		        ,lh.loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
		        ,lh.loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
		        ,d.deal_name --Наименование сделки
		        ,d.deal_sum --Сумма сделки
		        ,d.client_rk --Ссылка на контрагента
		        ,d.agreement_rk --Ссылка на договор
		        ,d.deal_start_date --Дата начала действия сделки
		        ,d.department_rk --Ссылка на ГО/филиал
		        ,d.product_rk -- Ссылка на продукт
		        ,p.product_name -- Наименование продукта
		        ,d.deal_type_cd -- Наименование типа сделки
		from deal d
		left join loan_holiday lh on 1=1
		                             and d.deal_rk = lh.deal_rk
		                             and d.effective_from_date = lh.effective_from_date
		left join product p on p.product_rk = d.product_rk
		),
		final_loan_holiday_info as (
		SELECT deal_rk
		      ,effective_from_date
		      ,effective_to_date
		      ,agreement_rk
			  ,account_rk
		      ,client_rk
		      ,department_rk
		      ,product_rk
		      ,product_name
		      ,deal_type_cd
		      ,deal_start_date
		      ,deal_name
		      ,deal_number
		      ,deal_sum
		      ,loan_holiday_type_cd
		      ,loan_holiday_start_date
		      ,loan_holiday_finish_date
		      ,loan_holiday_fact_finish_date
		      ,loan_holiday_finish_flg
		      ,loan_holiday_last_possible_date
		FROM holiday_info
		)
		insert into dm.loan_holiday_info (
										deal_rk,
										effective_from_date,
									    effective_to_date,
									    agreement_rk,
										account_rk,
									    client_rk,
									    department_rk,
									    product_rk,
									    product_name,
									    deal_type_cd,
									    deal_start_date,
									    deal_name,
									    deal_number,
									    deal_sum,
									    loan_holiday_type_cd,
									    loan_holiday_start_date,
									    loan_holiday_finish_date,
									    loan_holiday_fact_finish_date,
									    loan_holiday_finish_flg,
									    loan_holiday_last_possible_date
	    )
	    select
				f.deal_rk,
				f.effective_from_date,
			    f.effective_to_date,
			    f.agreement_rk,
				f.account_rk,
			    f.client_rk,
			    f.department_rk,
			    f.product_rk,
			    f.product_name,
			    f.deal_type_cd,
			    f.deal_start_date,
			    f.deal_name,
			    f.deal_number,
			    f.deal_sum,
			    f.loan_holiday_type_cd,
			    f.loan_holiday_start_date,
			    f.loan_holiday_finish_date,
			    f.loan_holiday_fact_finish_date,
			    f.loan_holiday_finish_flg,
			    f.loan_holiday_last_possible_date
	    from final_loan_holiday_info f
	    left join dm.loan_holiday_info d
			on f.deal_rk = d.deal_rk
			and f.effective_from_date = d.effective_from_date
	     	and f.effective_to_date = d.effective_to_date
	    where d.deal_rk is null;
		
		update dm.loan_holiday_info
		set product_name = deal_name
		where product_name is null
		  and deal_name is not null;

		
		delete 
		from dm.loan_holiday_info
		where ctid in (
		    select ctid
		    from (
		        select ctid,
		               row_number() over (
		                   partition by deal_rk, effective_from_date, effective_to_date
		                   order by ctid
		               ) as rn
		        from dm.loan_holiday_info
		    ) t
		    where rn > 1
		);
		
		end_time := clock_timestamp();
		duration := extract(epoch from (end_time - start_time))::INT;
	
		insert into logs.etl_log(operation_name,
								start_time,
								end_time,
								duration_sec)
		values('fill_loan_holiday_info', start_time, end_time, duration);
		
		exception
		    when others then
		        raise notice 'fill_loan_holiday_info error: %', sqlerrm;
end;
$$ language plpgsql;


call dm.fill_loan_holiday_info();
