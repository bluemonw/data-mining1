import dask.dataframe as dd
import time
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import time
start = time.time()
ddf = dd.read_parquet(
    "30G_data/*.parquet",
    engine='pyarrow',
    columns=['last_login']
)
ddf['last_login'] = dd.to_datetime(ddf['last_login'], errors='coerce')
ddf = ddf.dropna(subset=['last_login'])
daily_counts = (
    ddf['last_login']
    .dt.floor('D')
    .value_counts()
    .compute()
    .sort_index()
)
plt.figure(figsize=(15, 6))
daily_counts.plot(kind='line', color='steelblue')
plt.title('Daily Record Counts Over Time')
plt.xlabel('Date')
plt.ylabel('Number of Records')
plt.grid(alpha=0.3)
plt.tight_layout()
end = time.time()
print(f'Total processing time: {end-start:.2f} seconds')
plt.show()
