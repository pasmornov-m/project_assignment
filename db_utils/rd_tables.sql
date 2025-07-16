
CREATE TABLE IF NOT EXISTS rd.deal_info (
    deal_rk bigint NOT NULL,
    deal_num text,
    deal_name text,
    deal_sum numeric,
    client_rk bigint NOT NULL,
    account_rk bigint NOT NULL,
    agreement_rk bigint NOT NULL,
    deal_start_date date,
    department_rk bigint,
    product_rk bigint,
    deal_type_cd text,
    effective_from_date date NOT NULL,
    effective_to_date date NOT NULL
);


CREATE TABLE IF NOT EXISTS rd.loan_holiday (
    deal_rk bigint NOT NULL,
    loan_holiday_type_cd text,
    loan_holiday_start_date date,
    loan_holiday_finish_date date,
    loan_holiday_fact_finish_date date,
    loan_holiday_finish_flg boolean,
    loan_holiday_last_possible_date date,
    effective_from_date date NOT NULL,
    effective_to_date date NOT NULL
);


CREATE TABLE IF NOT EXISTS rd.product (
    product_rk bigint NOT NULL,
    product_name text,
    effective_from_date date NOT NULL,
    effective_to_date date NOT NULL
);