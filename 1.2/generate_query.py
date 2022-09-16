from datetime import datetime, timedelta

d = datetime.strptime("2018-01-01", "%Y-%m-%d")
for i in range(0, 31):

    s = (d  + timedelta(days=i)).date()
    print(f"call dm.fill_f101_round_f('{s}');")

for i in range(0, 31):

    s = (d  + timedelta(days=i)).date()
    print(f"call ds.fill_account_turnover_f('{s}');")


    