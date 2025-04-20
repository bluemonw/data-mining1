import dask.dataframe as dd
import time
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import time
import numpy as np
import plotly.express as px
from tabulate import tabulate
import seaborn as sns
start = time.time()
ddf = dd.read_parquet(
    "30G_data/*.parquet",
    columns=['age'],
    engine='pyarrow'
)
age_counts = ddf['age'].value_counts().compute()
age_counts = age_counts.sort_index()
plt.figure(figsize=(12, 6))
plt.bar(age_counts.index, age_counts.values, color='skyblue')
plt.xlabel('age')
plt.ylabel('number of people')
plt.title('Age distribution')
plt.xticks(age_counts.index, rotation=90)
plt.tight_layout()
plt.show()
end = time.time()
print(f'Total processing time: {end-start:.2f} seconds')