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


// tokenisation code snippet for managign sensitive PII values
// start with primary keys that exist as foreign key columns in other tables such as subscriber Ids
// code is straightforward to follow. we use monotonically_increasing_id() to create new indexes to replace the original values

val subscriberPath = ""
val subs = spark.read.parquet(subscriberPath)
val targetCol = "subId"
val subId_map = subs.select(targetCol)
                    .distinct
                    .withColumn("new_" + targetCol,
                      concat(lit(targetCol + "_"), (monotonically_increasing_id().cast(StringType))))

subId_map.write.parquet("tmp/subId_map")
val subs2 = subs.join(subId_map, Seq(targetCol), "left")
	.na.fill("unmatched", Seq("new_" + targetCol))
	.withColumn(targetCol, col("new_" + targetCol))
        .drop("new_" + targetCol)


// unpivot a dataframe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr

/*
 * given input dataframe
 * | t   | header1 | header2 | header3 |
 * |-----|---------|---------|---------|
 * |test |   20.0  |   30.0  |   40.0  |
 *
 * function will return dataframe
 * | t   | col0    | col1  |
 * |-----|---------|-------|
 * |test | header1 | 20.0  |
 * |test | header2 | 30.0  |
 * |test | header3 | 40.0  |
 *
 * @param df input dataframe
 * @param headers columns to transpose. They must be of same types e.g. String and Double columns should not be mixed together
 * @param unpivotKey Optional column to retain. in the e.g. above, t will be the unpivotKey
 * @return unpivoted dataframe with col0 and col1 representing headers and values respectively
 */
def unpivot(
  df: DataFrame,
  headers: List[String],
  unpivotKey: Option[String] = None): DataFrame = {

  val headersLen = headers.length.toString
  // stack expression example: stack(2, 'c1', c1 , 'c2', c2)
  val stackExpr = s"stack(${headersLen}, ${headers.map(h => s"'${h}', ${h}").mkString(", ")})"

  unpivotKey match {
    case Some(colName) => df.select(col(colName), expr(stackExpr))
    case None => df.select(expr(stackExpr))
  }
}



// timer to measure code execution duration

class Timer(startTime: Long = System.currentTimeMillis()) extends Serializable {

  private def currentTime: Long = System.currentTimeMillis()

  def stop: Long = currentTime - startTime

  def getTimeElapsedStr:String = "%1.2f".format(this.stop / 1000.0 / 60.0) + " minutes" // fmt two d.p.

}

// assuming we already have a logger initialised
log.info(s"${appName} job begin!")
val timer = new Timer()
// process some code
log.info(s"${appName} completed. Duration: " + timer.getTimeElapsedStr)
