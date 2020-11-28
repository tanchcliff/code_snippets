

// Scala code to format numerical columns of dataframes.
// Adds commas and removes exponential notations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, IntegerType, DecimalType, LongType}

def fmt(df:DataFrame, decimalPlaces:Int = 2): DataFrame = {
  df.schema.filter(x => x.dataType == DoubleType || x.dataType == LongType || x.dataType == IntegerType || x.dataType == DecimalType).foldLeft(df) {
    case (acc, col) => acc.withColumn(col.name, format_number(df(col.name), decimalPlaces))
  }
}

