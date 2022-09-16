

CREATE_FT_BALANCE_F = '''
CREATE TABLE IF NOT EXISTS DS.FT_BALANCE_F
( 
    on_date DATE not null,
    account_rk Numeric not null,
    currency_rk Numeric,
    balance_out FLOAT,
    PRIMARY KEY (on_date, account_rk)
    );
'''

CREATE_FT_POSTING_F = '''
CREATE TABLE IF NOT exists DS.FT_POSTING_F
(
    oper_date DATE not null,
    credit_account_rk Numeric not null,
    debet_account_rk Numeric not null,
    credit_amount FLOAT,
    debet_amount FLOAT,
    PRIMARY KEY(oper_date, credit_account_rk, debet_account_rk)
);
'''


CREATE_MD_ACCOUNT_D = '''CREATE TABLE IF NOT exists DS.MD_ACCOUNT_D
(
    data_actual_date DATE not null,
    data_actual_end_date DATE not null,
    account_rk Numeric not null,
    account_number VARCHAR(20) not null,
    char_type VARCHAR(1) not null,
    currency_rk Numeric not null,
    currency_code VARCHAR(4) not null,
    PRIMARY KEY(data_actual_date, account_rk)
);
'''
CREATE_MD_CURRENCY_D = '''
CREATE TABLE IF NOT exists DS.MD_CURRENCY_D
(
    currency_rk Numeric not null,
    data_actual_date DATE not null,
    data_actual_end_date DATE,
    currency_code VARCHAR(14),
    code_iso_char VARCHAR(14),
    PRIMARY KEY(currency_rk, data_actual_date)
);
'''
CREATE_MD_EXCHANGE_RATE_D = '''
CREATE table IF NOT EXISTS DS.MD_EXCHANGE_RATE_D
(
    data_actual_date DATE not null,
    data_actual_end_date DATE,
    currency_rk Numeric not null,
    reduced_cource FLOAT,
    code_iso_num VARCHAR,
    PRIMARY KEY(data_actual_date, currency_rk)
);
'''

CREATE_MD_LEDGER_ACCOUNT_S = '''
CREATE TABLE IF NOT exists DS.MD_LEDGER_ACCOUNT_S
(
    chapter CHAR(3),
    chapter_name VARCHAR(26),
    section_number INTEGER,
    section_name VARCHAR(32),
    subsection_name VARCHAR(31),
    ledger1_account INTEGER,
    ledger1_account_name VARCHAR(57),
    ledger_account INTEGER not null,
    ledger_account_name VARCHAR(173),
    characteristic CHAR(10),
    is_resident INTEGER,
    is_reserve INTEGER,
    is_reserved INTEGER,
    is_loan INTEGER,
    is_reserved_assets INTEGER,
    is_overdue INTEGER,
    is_interest INTEGER,
    pair_account VARCHAR(5),
    start_date DATE not null,
    end_date DATE,
    is_rub_only INTEGER,
    min_term VARCHAR(3),
    min_term_measure VARCHAR(3),
    max_term VARCHAR(3),
    max_term_measure VARCHAR(3),
    ledger_acc_full_name_translit VARCHAR(3),
    is_revaluation VARCHAR(3),
    is_correct VARCHAR(3),
    PRIMARY KEY(ledger_account, start_date)
);
'''



INSERT_FT_BALANCE_F = '''INSERT INTO DS.FT_BALANCE_F (on_date, account_rk, currency_rk, balance_out) VALUES 
QUERY
ON CONFLICT (on_date, account_rk) DO
                        UPDATE
                        SET currency_rk = excluded.currency_rk,
                        balance_out = excluded.balance_out;'''


INSERT_FT_POSTING_F = '''INSERT INTO DS.FT_POSTING_F (oper_date, credit_account_rk, debet_account_rk, credit_amount, debet_amount) VALUES
QUERY
ON CONFLICT (oper_date, credit_account_rk, debet_account_rk) DO
                        UPDATE
                        SET credit_amount = excluded.credit_amount,
                        debet_amount = excluded.debet_amount;
'''




INSERT_MD_ACCOUNT_D = '''INSERT INTO DS.MD_ACCOUNT_D (data_actual_date, data_actual_end_date, account_rk, account_number, char_type, currency_rk, currency_code) VALUES
QUERY
ON CONFLICT (data_actual_date, account_rk) DO
UPDATE
SET data_actual_end_date = excluded.data_actual_end_date,
account_number = excluded.account_number,
char_type = excluded.char_type,
currency_rk = excluded.currency_rk,
currency_code = excluded.currency_code;
'''

INSERT_MD_CURRENCY_D = '''INSERT INTO DS.MD_CURRENCY_D (currency_rk, data_actual_date, data_actual_end_date, currency_code, code_iso_char) VALUES
QUERY
ON CONFLICT (currency_rk, data_actual_date) DO
UPDATE
SET data_actual_end_date = excluded.data_actual_end_date,
currency_code = excluded.currency_code,
code_iso_char = excluded.code_iso_char;
'''

INSERT_MD_EXCHANGE_RATE_D = '''INSERT INTO DS.MD_EXCHANGE_RATE_D (data_actual_date, data_actual_end_date, currency_rk, reduced_cource, code_iso_num) VALUES
QUERY
ON CONFLICT (data_actual_date, currency_rk) DO
UPDATE
SET data_actual_end_date = excluded.data_actual_end_date,
reduced_cource = excluded.reduced_cource,
code_iso_num = excluded.code_iso_num;
'''

INSERT_MD_LEDGER_ACCOUNT_S = '''INSERT INTO DS.MD_LEDGER_ACCOUNT_S (chapter, chapter_name, section_number, section_name, subsection_name, ledger1_account, ledger1_account_name, ledger_account,ledger_account_name, characteristic,is_resident,is_reserve,is_reserved,is_loan,is_reserved_assets,is_overdue,is_interest,pair_account,start_date,end_date,is_rub_only,min_term,min_term_measure,max_term,max_term_measure,ledger_acc_full_name_translit,is_revaluation,is_correct) VALUES
QUERY
ON CONFLICT (ledger_account, start_date) DO
UPDATE SET 
chapter = excluded.chapter,
chapter_name = excluded.chapter_name,
section_number = excluded.section_number,
section_name = excluded.section_name,
subsection_name = excluded.subsection_name,
ledger1_account = excluded.ledger1_account,
ledger1_account_name = excluded.ledger1_account_name,
characteristic = excluded.characteristic,
is_resident = excluded.is_resident,
is_reserve = excluded.is_reserve,
is_reserved = excluded.is_reserved,
is_reserved_assets = excluded.is_reserved_assets,
is_overdue = excluded.is_overdue,
is_interest = excluded.is_interest,
pair_account = excluded.pair_account,
end_date = excluded.end_date,
is_rub_only = excluded.is_rub_only,
min_term_measure = excluded.min_term_measure,
max_term_measure = excluded.max_term_measure,
ledger_acc_full_name_translit = excluded.ledger_acc_full_name_translit,
is_revaluation = excluded.is_revaluation,
min_term = excluded.min_term,
is_correct = excluded.is_correct;
'''


CREATE_LOG = '''
CREATE TABLE IF NOT EXISTS logs.logs
( 
    on_date DATE not null,
    status VARCHAR not null,
    message VARCHAR not null
    );
'''

INSERT_LOG = '''
insert into logs.logs (on_date, status, message) 
    VALUES ('{{ params.on_date }}', '{{ params.status }}', '{{ params.message }}' 
)

'''




SYS = {
    'ft_balance_f': [CREATE_FT_BALANCE_F, INSERT_FT_BALANCE_F],
    'ft_posting_f': [CREATE_FT_POSTING_F, INSERT_FT_POSTING_F],
    'md_account_d': [CREATE_MD_ACCOUNT_D, INSERT_MD_ACCOUNT_D],
    'md_currency_d': [CREATE_MD_CURRENCY_D, INSERT_MD_CURRENCY_D],
    'md_exchange_rate_d': [CREATE_MD_EXCHANGE_RATE_D, INSERT_MD_EXCHANGE_RATE_D],
    'md_ledger_account_s': [CREATE_MD_LEDGER_ACCOUNT_S, INSERT_MD_LEDGER_ACCOUNT_S]
}
