import dask.dataframe as dd
import time
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import time
import numpy as np
start = time.time()
import seaborn as sns
start = time.time()
ddf = dd.read_parquet(
    "30G_data/*.parquet",
    engine='pyarrow',
    columns=['registration_date', 'is_active']
)
ddf['registration_date'] = dd.to_datetime(ddf['registration_date'])
ddf['registration_year'] = ddf.registration_date.dt.year

grouped = ddf.groupby(['registration_year', 'is_active']).size().compute()
grouped = grouped.unstack(fill_value=0)

years = grouped.index.tolist()
n_years = len(years)
n_cols = 3
n_rows = (n_years + n_cols - 1) // n_cols

fig, axes = plt.subplots(n_rows, n_cols, figsize=(20, 5 * n_rows))
axes = axes.flatten()

for i, year in enumerate(years):
    ax = axes[i]
    active = grouped.loc[year, True]
    inactive = grouped.loc[year, False]
    total = active + inactive

    if total == 0:
        ax.text(0.5, 0.5, 'No Data', ha='center', va='center')
        ax.set_title(f'Year {year}')
        continue

    ax.pie(
        [active, inactive],
        labels=['Active', 'Inactive'],
        autopct=lambda p: f'{p:.1f}%\n({int(p * total / 100)})',
        startangle=90,
        colors=['#66b3ff', '#ff9999']
    )
    ax.set_title(f'Year {year} - Total: {total}')
for j in range(i + 1, len(axes)):
    axes[j].axis('off')
plt.tight_layout()
plt.show()
end = time.time()
print(f'Total processing time: {end-start:.2f} seconds')