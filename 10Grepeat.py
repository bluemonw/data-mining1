import dask.dataframe as dd
import time
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import time
start = time.time()
ddf = dd.read_parquet(
    "10G_data/*.parquet",
    engine='pyarrow'
)
counts = ddf["user_name"].value_counts().compute()
duplicated_users = counts[counts > 1].index.tolist()
duplicated_df = ddf[ddf["user_name"].isin(duplicated_users)]
num_duplicates = len(duplicated_df) - len(duplicated_users)
total_users = ddf["user_name"].count().compute()
duplicate_ratio = num_duplicates / total_users

ddf_clean = ddf.drop_duplicates(subset=["user_name"], keep="first")
print(f"重复值的数量: {num_duplicates}")
print(f"总行数: {total_users}")
print(f"重复值的比例: {duplicate_ratio:.2f}%")
end = time.time()
print(f'Total processing time: {end-start:.2f} seconds')
