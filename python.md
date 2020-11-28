# Python / Pandas codes


correlation between two columns in pandas dataframe

```
col1 = df['col_name_1']
col2 = df['col_name_2']
correlation = col1.corr(col2)

print(correlation)
```

use fastparquet to read parquet files and convert into pandas dataframe
```
import pandas as pd
from fastparquet import ParquetFile

pf = ParquetFile('raw_cell_thpt')
df = pf.to_pandas()
```

drop rows with null values
```
df.dropna(inplace=True)
```

convert string to datetime
```
df['dateTime'] = pd.to_datetime(df['dateTime'])
```

simple filters
```
df2 = df.query(" userCount > 10 & weather == 'cloudy' ") # and condition
df3 = df.query(" userCount <= 10 | weather == 'sunny' ") # OR condition
df4 = df.query(" hour in ['01', '02']") # list comparison, exist in
df5 = df.query(" hour not in ['01', '02']") # list comparison, do not exist in
```

count of lines
```
df.count()
df.size() returns row count * column count
df.shape[0]
```

sort
```
df.sort_values(['month', 'dayType'])
```

unique
```
df.drop_duplicates()
```

join dataframes
```
pd.merge(df1, df2, on='key')
pd.merge(df1, df2, on=['key1', 'key2'])
pd.merge(df1, df2, on='col1', how='outer', indicator=True)
```

