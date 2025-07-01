-- Active: 1750861427417@@127.0.0.1@5432@project

CREATE DATABASE project

-- DS 

CREATE SCHEMA IF NOT EXISTS DS

DROP TABLE IF EXISTS DS.FT_BALANCE_F CASCADE;
CREATE TABLE IF NOT EXISTS DS.FT_BALANCE_F (
    on_date DATE NOT NULL,
    account_rk INT NOT NULL,
    currency_rk INT,
    balance_out FLOAT,
    PRIMARY KEY (on_date, account_rk)
);


DROP TABLE IF EXISTS DS.FT_POSTING_F CASCADE;
CREATE TABLE IF NOT EXISTS DS.FT_POSTING_F (
    oper_date DATE NOT NULL,
    credit_account_rk INT NOT NULL,
    debet_account_rk INT NOT NULL,
    credit_amount FLOAT,
    debet_amount FLOAT
);


DROP TABLE IF EXISTS DS.MD_ACCOUNT_D CASCADE;
CREATE TABLE IF NOT EXISTS DS.MD_ACCOUNT_D (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE NOT NULL,
    account_rk INT NOT NULL,
    account_number VARCHAR(20) NOT NULL,
    char_type VARCHAR(1) NOT NULL,
    currency_rk INT NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    PRIMARY KEY (data_actual_date, account_rk)
);


DROP TABLE IF EXISTS DS.MD_CURRENCY_D CASCADE;
CREATE TABLE IF NOT EXISTS DS.MD_CURRENCY_D (
    currency_rk INT NOT NULL,
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_code VARCHAR(3),
    code_iso_char VARCHAR(3),
    PRIMARY KEY (currency_rk, data_actual_date)
);


DROP TABLE IF EXISTS DS.MD_EXCHANGE_RATE_D CASCADE;
CREATE TABLE IF NOT EXISTS DS.MD_EXCHANGE_RATE_D (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_rk INT NOT NULL,
    reduced_cource FLOAT,
    code_iso_num VARCHAR(3),
    PRIMARY KEY (data_actual_date, currency_rk)
);


DROP TABLE IF EXISTS DS.MD_LEDGER_ACCOUNT_S CASCADE;
CREATE TABLE IF NOT EXISTS DS.MD_LEDGER_ACCOUNT_S (
    chapter CHAR(1),
    chapter_name VARCHAR(16),
    section_number INT,
    section_name VARCHAR(22),
    subsection_name VARCHAR(21),
    ledger1_account INT,
    ledger1_account_name VARCHAR(47),
    ledger_account INT NOT NULL,
    ledger_account_name VARCHAR(153),
    characteristic CHAR(1),
    is_resident INT,
    is_reserve INT,
    is_reserved INT,
    is_loan INT,
    is_reserved_assets INT,
    is_overdue INT,
    is_interest INT,
    pair_account VARCHAR(5),
    start_date DATE NOT NULL,
    end_date DATE,
    is_rub_only INT,
    min_term VARCHAR(1),
    min_term_measure VARCHAR(1),
    max_term VARCHAR(1),
    max_term_measure VARCHAR(1),
    ledger_acc_full_name_translit VARCHAR(1),
    is_revaluation VARCHAR(1),
    is_correct VARCHAR(1),
    PRIMARY KEY (ledger_account, start_date)
);


-- LOGS

CREATE SCHEMA IF NOT EXISTS LOGS

CREATE TABLE IF NOT EXISTS LOGS.UPLOAD_LOGS (
    upload_id SERIAL PRIMARY KEY,
    start_upload TIMESTAMP  NOT NULL,
    end_upload TIMESTAMP  NOT NULL
)