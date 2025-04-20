import dask.dataframe as dd
import time
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import time

from dask import compute

start = time.time()
ddf = dd.read_parquet(
    "30G_data/*.parquet",
    engine='pyarrow'
)
missing={}
columns = ddf.columns.tolist()
for col in columns:
    missing_count = sum(
             part.isna().sum()
             for part in ddf[col].to_delayed()
     )
    total_count = sum(
        part.count()
        for part in ddf[col].to_delayed()
    )
    missing_count, total_count = compute(missing_count, total_count)
    print(col,end="")
    print("缺失值数量：",end="")
    print(missing_count,end=" ")
    print(col, end="")
    print("总行数：", end="")
    print(total_count)
    missing_ratio = (missing_count / total_count) * 100
    missing[col] = {
        'missing_count': missing_count,
        'total_count': total_count,
        'missing_ratio': missing_ratio
    }
print("每列的缺失值数量和比例如下：")
for col, info in missing.items():
    print(f"{col}: 缺失值数量 = {info['missing_count']}, 总行数 = {info['total_count']}, 缺失值比例 = {info['missing_ratio']:.2f}%")
for col in columns:
    ddf[col] = ddf[col].fillna('unknown')
end = time.time()
print(f'Total processing time: {end-start:.2f} seconds')
