import random 
import pandas as pd 
from datetime import datetime ,timedelta 


records = []

for i in range(1,50):
    orders_date = datetime(
        2018,
        random.randint(1,12),
        random.randint(1,28),
        random.randint(0,23),
        random.randint(0,59),
        random.randint(0,59)
    )

records.append({
    "order_id":f"LATE_{i}",
    "custome_id":f"CUST_{i}",
    "order_purchase_timestamp":orders_date.strftime("%Y-%m-%d %H:%M:%S"),
    "order_status":"delivered"
})

df =pd.DataFrame(records)

df.to_csv(
    "data/raw/late_orders.csv",
    index=False
)

print("created 50 late orders csv files in data folder ")