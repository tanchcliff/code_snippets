
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
