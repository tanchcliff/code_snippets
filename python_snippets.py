
# use fastparquet to read parquet files and convert into pandas dataframe

import pandas as pd
from fastparquet import ParquetFile

pf = ParquetFile('raw_cell_thpt')
df = pf.to_pandas()


