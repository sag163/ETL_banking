    create or replace procedure dm.fill_f101_round_f ( 
  i_OnDate  date
)
language plpgsql    
as $$
declare
	v_RowCount int;
begin
    call dm.writelog( '[BEGIN] fill(i_OnDate => date ''' 
         || to_char(i_OnDate, 'yyyy-mm-dd') 
         || ''');', 1
       );
    
    call dm.writelog( 'delete on_date = ' 
         || to_char(i_OnDate, 'yyyy-mm-dd'), 1
       );

    delete
      from dm.DM_F101_ROUND_F f
     where from_date = date_trunc('month', i_OnDate)  
       and to_date = (date_trunc('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day');
   
    call dm.writelog('insert', 1);
   
    insert 
      into dm.dm_f101_round_f
           ( from_date         
           , to_date           
           , chapter           
           , ledger_account    
           , characteristic    
           , balance_in_rub    
           , balance_in_val    
           , balance_in_total  
           , turn_deb_rub      
           , turn_deb_val      
           , turn_deb_total    
           , turn_cre_rub      
           , turn_cre_val      
           , turn_cre_total    
           , balance_out_rub  
           , balance_out_val   
           , balance_out_total
           )
    select  date_trunc('month', i_OnDate)        as from_date,
           (date_trunc('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day')  as to_date,
           s.chapter                             as chapter,
           substr(acc_d.account_number, 1, 5)    as ledger_account,
           acc_d.char_type                       as characteristic,
           -- RUB balance
           sum( case 
                  when cur.currency_code in ('643', '810')
                  then b.balance_out
                  else 0
                 end
              )                                  as balance_in_rub,
          -- VAL balance converted to rub
          sum( case 
                 when cur.currency_code not in ('643', '810')
                 then b.balance_out * exch_r.reduced_cource
                 else 0
                end
             )                                   as balance_in_val,
          -- Total: RUB balance + VAL converted to rub
          sum(  case 
                 when cur.currency_code in ('643', '810')
                 then b.balance_out
                 else b.balance_out * exch_r.reduced_cource
               end
             )                                   as balance_in_total  ,
           -- RUB debet turnover
           sum(case 
                 when cur.currency_code in ('643', '810')
                 then at.debet_amount_rub
                 else 0
               end
           )                                     as turn_deb_rub,
           -- VAL debet turnover converted
           sum(case 
                 when cur.currency_code not in ('643', '810')
                 then at.debet_amount_rub
                 else 0
               end
           )                                     as turn_deb_val,
           -- SUM = RUB debet turnover + VAL debet turnover converted
           sum(at.debet_amount_rub)              as turn_deb_total,
           -- RUB credit turnover
           sum(case 
                 when cur.currency_code in ('643', '810')
                 then at.credit_amount_rub
                 else 0
               end
              )                                  as turn_cre_rub,
           -- VAL credit turnover converted
           sum(case 
                 when cur.currency_code not in ('643', '810')
                 then at.credit_amount_rub
                 else 0
               end
              )                                  as turn_cre_val,
           -- SUM = RUB credit turnover + VAL credit turnover converted
           sum(at.credit_amount_rub)             as turn_cre_total,
           
           
           sum( case 
                  when  acc_d.char_type = 'A' and cur.currency_code in ('643', '810')
                  then b.balance_out -  - at.credit_amount_rub + at.debet_amount_rub
                  when acc_d.char_type = 'P' and cur.currency_code in ('643', '810')
                  then b.balance_out + at.credit_amount_rub - at.debet_amount_rub
                  else 0
                 end 
              )                                  as balance_out_rub,
              
            sum( case 
                  when acc_d.char_type = 'A' and cur.currency_code not in ('643', '810')
                  then b.balance_out * exch_r.reduced_cource - at.credit_amount_rub + at.debet_amount_rub
                  when acc_d.char_type = 'P' and cur.currency_code not in ('643', '810')
                  then b.balance_out * exch_r.reduced_cource + at.credit_amount_rub - at.debet_amount_rub
                  else 0
                 end
              )                                  as balance_out_val,
              
             sum(case 
                  when  acc_d.char_type = 'A'
                  then case 
                  when cur.currency_code in ('643', '810')
                  then b.balance_out -  - at.credit_amount_rub + at.debet_amount_rub
                  else b.balance_out * exch_r.reduced_cource - at.credit_amount_rub + at.debet_amount_rub
                  end
             
                  when acc_d.char_type = 'P'
                  then case 
                  when cur.currency_code in ('643', '810')
                  then b.balance_out + at.credit_amount_rub - at.debet_amount_rub
                  else b.balance_out * exch_r.reduced_cource + at.credit_amount_rub - at.debet_amount_rub
                  end
                  else 0
				end             
             )
                                                 as balance_out_total
                                                       
      from ds.md_ledger_account_s s
      join ds.md_account_d acc_d
        on substr(acc_d.account_number, 1, 5) = to_char(s.ledger_account, 'FM99999999')
      join ds.md_currency_d cur
        on cur.currency_rk = acc_d.currency_rk
      left 
      join ds.ft_balance_f b
        on b.account_rk = acc_d.account_rk
       and b.on_date  = (date_trunc('month', i_OnDate) - INTERVAL '1 day')
      left 
      join ds.md_exchange_rate_d exch_r
        on exch_r.currency_rk = acc_d.currency_rk
       and i_OnDate between exch_r.data_actual_date and exch_r.data_actual_end_date
      left 
      join dm.dm_account_turnover_f at
        on at.account_rk = acc_d.account_rk
       and at.on_date between date_trunc('month', i_OnDate) and (date_trunc('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day')
     where i_OnDate between s.start_date and s.end_date
       and i_OnDate between acc_d.data_actual_date and acc_d.data_actual_end_date
       and i_OnDate between cur.data_actual_date and cur.data_actual_end_date
     group by s.chapter,
           substr(acc_d.account_number, 1, 5),
           acc_d.char_type;
	
	GET DIAGNOSTICS v_RowCount = ROW_COUNT;
    call dm.writelog('[END] inserted ' ||  to_char(v_RowCount,'FM99999999') || ' rows.', 1);

    commit;
    
  end;$$
  
  



delete  from dm.dm_f101_round_f

delete from dm.lg_messages

select *  from dm.lg_messages

select *  from dm.dm_f101_round_f


call dm.fill_f101_round_f('2018-01-01');
call dm.fill_f101_round_f('2018-01-02');
call dm.fill_f101_round_f('2018-01-03');
call dm.fill_f101_round_f('2018-01-04');
call dm.fill_f101_round_f('2018-01-05');
call dm.fill_f101_round_f('2018-01-06');
call dm.fill_f101_round_f('2018-01-07');
call dm.fill_f101_round_f('2018-01-08');
call dm.fill_f101_round_f('2018-01-09');
call dm.fill_f101_round_f('2018-01-10');
call dm.fill_f101_round_f('2018-01-11');
call dm.fill_f101_round_f('2018-01-12');
call dm.fill_f101_round_f('2018-01-13');
call dm.fill_f101_round_f('2018-01-14');
call dm.fill_f101_round_f('2018-01-15');
call dm.fill_f101_round_f('2018-01-16');
call dm.fill_f101_round_f('2018-01-17');
call dm.fill_f101_round_f('2018-01-18');
call dm.fill_f101_round_f('2018-01-19');
call dm.fill_f101_round_f('2018-01-20');
call dm.fill_f101_round_f('2018-01-21');
call dm.fill_f101_round_f('2018-01-22');
call dm.fill_f101_round_f('2018-01-23');
call dm.fill_f101_round_f('2018-01-24');
call dm.fill_f101_round_f('2018-01-25');
call dm.fill_f101_round_f('2018-01-26');
call dm.fill_f101_round_f('2018-01-27');
call dm.fill_f101_round_f('2018-01-28');
call dm.fill_f101_round_f('2018-01-29');
call dm.fill_f101_round_f('2018-01-30');
call dm.fill_f101_round_f('2018-01-31');




call ds.fill_account_turnover_f('2018-01-01');
call ds.fill_account_turnover_f('2018-01-02');
call ds.fill_account_turnover_f('2018-01-03');
call ds.fill_account_turnover_f('2018-01-04');
call ds.fill_account_turnover_f('2018-01-05');
call ds.fill_account_turnover_f('2018-01-06');
call ds.fill_account_turnover_f('2018-01-07');
call ds.fill_account_turnover_f('2018-01-08');
call ds.fill_account_turnover_f('2018-01-09');
call ds.fill_account_turnover_f('2018-01-10');
call ds.fill_account_turnover_f('2018-01-11');
call ds.fill_account_turnover_f('2018-01-12');
call ds.fill_account_turnover_f('2018-01-13');
call ds.fill_account_turnover_f('2018-01-14');
call ds.fill_account_turnover_f('2018-01-15');
call ds.fill_account_turnover_f('2018-01-16');
call ds.fill_account_turnover_f('2018-01-17');
call ds.fill_account_turnover_f('2018-01-18');
call ds.fill_account_turnover_f('2018-01-19');
call ds.fill_account_turnover_f('2018-01-20');
call ds.fill_account_turnover_f('2018-01-21');
call ds.fill_account_turnover_f('2018-01-22');
call ds.fill_account_turnover_f('2018-01-23');
call ds.fill_account_turnover_f('2018-01-24');
call ds.fill_account_turnover_f('2018-01-25');
call ds.fill_account_turnover_f('2018-01-26');
call ds.fill_account_turnover_f('2018-01-27');
call ds.fill_account_turnover_f('2018-01-28');
call ds.fill_account_turnover_f('2018-01-29');
call ds.fill_account_turnover_f('2018-01-30');
call ds.fill_account_turnover_f('2018-01-31');



select * from dm.dm_account_turnover_f

update dm.dm_account_turnover_f set credit_amount_rub  = 0 where credit_amount_rub is null;
update dm.dm_account_turnover_f set credit_amount  = 0 where credit_amount is null;
update dm.dm_account_turnover_f set debet_amount_rub  = 0 where debet_amount_rub is null;
update dm.dm_account_turnover_f set debet_amount  = 0 where debet_amount is null;

























delete  from dm.dm_account_turnover_f

create or replace procedure ds.fill_account_turnover_f (
   i_OnDate date
)
language plpgsql    
as $$
declare
	v_RowCount int;
begin
	
	call dm.writelog( '[BEGIN] fill(i_OnDate => date ''' 
         || to_char(i_OnDate, 'yyyy-mm-dd') 
         || ''');', 1
       );
    
    call dm.writelog( 'delete on_date = ' 
         || to_char(i_OnDate, 'yyyy-mm-dd'), 1
       );
	   
    delete
      from dm.dm_account_turnover_f f
     where f.on_date = i_OnDate;
   
    call dm.writelog('insert', 1);
	
    insert
      into dm.dm_account_turnover_f
           ( on_date
           , account_rk
           , credit_amount
           , credit_amount_rub
           , debet_amount
           , debet_amount_rub
           )
    with wt_turn as
    ( select p.credit_account_rk                  as account_rk
           , p.credit_amount                      as credit_amount
           , p.credit_amount * nullif(er.reduced_cource, 1)         as credit_amount_rub
           , cast(null as numeric)                 as debet_amount
           , cast(null as numeric)                 as debet_amount_rub
        from ds.ft_posting_f p
        join ds.md_account_d a
          on a.account_rk = p.credit_account_rk
        left
        join ds.md_exchange_rate_d er
          on er.currency_rk = a.currency_rk
         and i_OnDate between er.data_actual_date   and er.data_actual_end_date
       where p.oper_date = i_OnDate
         and i_OnDate           between a.data_actual_date    and a.data_actual_end_date
         and a.data_actual_date between date_trunc('month', i_OnDate) and (date_trunc('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day')
       union all
      select p.debet_account_rk                   as account_rk
           , cast(null as numeric)                 as credit_amount
           , cast(null as numeric)                 as credit_amount_rub
           , p.debet_amount                       as debet_amount
           , p.debet_amount * nullif(er.reduced_cource, 1)          as debet_amount_rub
        from ds.ft_posting_f p
        join ds.md_account_d a
          on a.account_rk = p.debet_account_rk
        left 
        join ds.md_exchange_rate_d er
          on er.currency_rk = a.currency_rk
         and i_OnDate between er.data_actual_date and er.data_actual_end_date
       where p.oper_date = i_OnDate
         and i_OnDate           between a.data_actual_date and a.data_actual_end_date
         and a.data_actual_date between date_trunc('month', i_OnDate) and (date_trunc('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day')
    )
    select i_OnDate                               as on_date
         , t.account_rk
         , sum(t.credit_amount)                   as credit_amount
         , sum(t.credit_amount_rub)               as credit_amount_rub
         , sum(t.debet_amount)                    as debet_amount
         , sum(t.debet_amount_rub)                as debet_amount_rub
      from wt_turn t
     group by t.account_rk;
	 
	GET DIAGNOSTICS v_RowCount = ROW_COUNT;
    call dm.writelog('[END] inserted ' || to_char(v_RowCount,'FM99999999') || ' rows.', 1);

    commit;
	
end;$$
