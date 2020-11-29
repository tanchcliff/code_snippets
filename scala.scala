// Scala codes

// scala code to reformat numerical columns of dataframes.
// adds commas and removes exponential notations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, IntegerType, DecimalType, LongType}

def fmt(df:DataFrame, decimalPlaces:Int = 2): DataFrame = {
  df.schema.filter(x => x.dataType == DoubleType || x.dataType == LongType || x.dataType == IntegerType || x.dataType == DecimalType).foldLeft(df) {
    case (acc, col) => acc.withColumn(col.name, format_number(df(col.name), decimalPlaces))
  }
}


// Custom defined datasets might throw schema errors during unit test case's assertion comparison operation against computed values 
// Below is a spark scala method to set dataframe columns to be nullable, it sets nullable property for each column of the data frame

def setNullableStateForAllColumns(df: DataFrame, nullable: Boolean): DataFrame = {
  df.sqlContext.createDataFrame(df.rdd, StructType(df.schema.map(_.copy(nullable = nullable))))
}




// when using python based fastparquet and pandas libaries in jupyter notebooks, you might encounter errors regarding the lack of meta data
// you can use the below scala snippet to save the meta data file and enable parsing by fastparquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter}
import org.apache.spark.{SparkConf, SparkContext}
import java.net.URI

val target_path = ""
val conf = sc.hadoopConfiguration
val uri = new URI(target_path)
val fs = FileSystem.get(uri, conf)
val footers = ParquetFileReader.readAllFootersInParallel(conf, fs.getFileStatus(new Path(target_path)))
ParquetFileWriter.writeMetadataFile(conf, new Path(target_path), footers)


