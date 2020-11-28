
# Zeppelin

use zeppelin context to pass dataset between spark scala and python contexts

```
%spark

// method to save spark df with preferred name
import org.apache.spark.sql.DataFrame

def addToZ(df:DataFrame, name:String) = {
    z.put(name, df)
}
```

```
%pyspark

# retrieve saved table as pandas dataframe and generate plot

from pyspark.sql import DataFrame
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats

def getFromZ(name):
  df = DataFrame(z.get(name), sqlContext)
  df.toPandas()

p = getFromZ("tableName")
p = p.set_index('variableX')
p = p['variableY']
sns.distplot(p)

```
