import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import dask.dataframe as dd
import numpy as np
from datetime import datetime
# 购买次数权重：0.4
# 收入权重：0.3
# 最后一次登录评分权重：0.2
# 活跃状态权重：0.1
import time
start = time.time()
ddf = dd.read_parquet(
    "10G_data/*.parquet",
    columns=['user_name', 'last_login', 'purchase_history',
             'income', 'is_active'],
    engine='pyarrow'
)
ddf['last_login'] = dd.to_datetime(ddf['last_login'], errors='coerce',utc=True)
max_last_login = ddf['last_login'].max().compute()

ddf['last_login_days'] = (max_last_login - ddf['last_login']).dt.days


ddf['purchase_count'] = ddf['purchase_history'].count()
purchase_count_min = ddf['purchase_count'].min().compute()
purchase_count_max = ddf['purchase_count'].max().compute()
ddf['purchase_count_normalized'] = (ddf['purchase_count'] - purchase_count_min) / (purchase_count_max - purchase_count_min)
income_min = ddf['income'].min().compute()
income_max = ddf['income'].max().compute()
ddf['income_normalized'] = (ddf['income'] - income_min) / (income_max - income_min)

ddf['is_active_score'] = ddf['is_active'].astype(int)
last_login_score_min = ddf['last_login_days'].min().compute()
last_login_score_max = ddf['last_login_days'].max().compute()
ddf['last_login_score_normalized'] = 1 - (ddf['last_login_days'] - last_login_score_min) / (last_login_score_max - last_login_score_min)

ddf['total_score'] = (
    0.4 * ddf['purchase_count_normalized'] +
    0.3 * ddf['income_normalized'] +
    0.2 * ddf['last_login_score_normalized'] +
    0.1 * ddf['is_active_score']
)
total_users = ddf['user_name'].count().compute()
top_10_percent = int(total_users * 0.1)


high_value_users = ddf.nlargest(top_10_percent, 'total_score', split_every=4)


high_value_users_df = high_value_users[['user_name', 'total_score', 'purchase_count', 'income', 'last_login', 'is_active']].compute()
print("高价值用户（前10%）如下：")
print(high_value_users_df)
end = time.time()
print(f'Total processing time: {end-start:.2f} seconds')