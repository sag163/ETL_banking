CREATE or replace FUNCTION get_max_min_param (i_OnDate date)
RETURNS TABLE(oper_date date, max_credit_amount int, min_credit_amount int, max_debet_amount int, min_debet_amount int) AS $$
select oper_date, max(credit_amount) as max_credit_amount, min(credit_amount) as min_credit_amount, 
        max(debet_amount) as max_debet_amount, min(debet_amount) as min_debet_amount
        from ds.ft_posting_f
group by oper_date
having oper_date = i_OnDate;
$$ LANGUAGE SQL;
