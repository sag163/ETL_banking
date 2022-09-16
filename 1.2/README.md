Задача 1.2
После того как детальный слой «DS» успешно наполнен исходными данными из файлов – нужно рассчитать витрины данных в слое «DM»: витрину оборотов и витрину 101-й отчётной формы. 
Для этого вам сперва необходимо построить витрину оборотов «DM.DM_ACCOUNT_TURNOVER_F». А именно, посчитать за каждый день января 2018 года кредитовые и дебетовые обороты по счетам с помощью Oracle-пакета dm.fill_account_turnover_f или с помощью аналогичной PostgreSQL-процедуры.
	Затем вы узнаёте от Аналитика в банке, что пакет (или процедуру) расчёта витрины 101-й формы «dm.fill_f101_round_f» необходимо доработать. Необходимо сделать расчёт полей витрины «dm.dm_f101_round_f» по формулам:
    • «BALANCE_OUT_RUB»
для счетов с CHARACTERISTIC = 'A' и currency_code '643' рассчитать BALANCE_OUT_RUB = BALANCE_IN_RUB - TURN_CRE_RUB + TURN_DEB_RUB;

для счетов с CHARACTERISTIC = 'A' и currency_code '810' рассчитать BALANCE_OUT_RUB = BALANCE_IN_RUB - TURN_CRE_RUB + TURN_DEB_RUB;

для счетов с CHARACTERISTIC = 'P' и currency_code '643' рассчитать BALANCE_OUT_RUB = BALANCE_IN_RUB + TURN_CRE_RUB - TURN_DEB_RUB;

для счетов с CHARACTERISTIC = 'P' и currency_code '810' рассчитать BALANCE_OUT_RUB = BALANCE_IN_RUB + TURN_CRE_RUB - TURN_DEB_RUB;

    • «BALANCE_OUT_VAL»
для счетов с CHARACTERISTIC = 'A' и currency_code не '643' и не '810'  рассчитать BALANCE_OUT_VAL = BALANCE_IN_VAL - TURN_CRE_VAL + TURN_DEB_VAL;

для счетов с CHARACTERISTIC = 'P' и currency_code не '643' и не '810'  рассчитать BALANCE_OUT_VAL = BALANCE_IN_VAL + TURN_CRE_VAL - TURN_DEB_VAL;

    • «BALANCE_OUT_TOTAL»
рассчитать BALANCE_OUT_TOTAL как BALANCE_OUT_VAL + BALANCE_OUT_RUB   
