package fetcher

import common.sparkSetting._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext._
import serde.HBaseSerializable

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 10/22/13
 * Time: 6:09 PM
 */


object hbaseFetcher {

  def creatConf = {
    val conf = HBaseConfiguration.create()

    // general hbase setting
    conf.set("hbase.rootdir", "hdfs://" + nameNodeURL + ":" + hdfsPort + "/hbase")
    conf.setBoolean("hbase.cluster.distributed", true)
    conf.set("hbase.zookeeper.quorum", hostname)
    conf.setInt("hbase.client.scanner.caching", 10000)
    conf
  }

  def readData(tableName: String) = {
    val conf = creatConf
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      throw new Exception("The table " + tableName + " does not exist in HBase")
    }

    val hBaseRDD = sparkContext.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    hBaseRDD.map(_._2)
  }

  def writeData[T <: HBaseSerializable](srcRdd: RDD[T], outputTable: String) = {
    val jobConfig: JobConf = new JobConf(hbaseFetcher.creatConf, this.getClass)

    // Note:  TableOutputFormat is used as deprecated code, because JobConf is in old API
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, outputTable)

    srcRdd.map(_.toKeyValuePair).saveAsHadoopDataset(jobConfig)
  }
}