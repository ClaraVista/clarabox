package serde

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/10/13
 * Time: 10:32 AM
 */

trait HBaseSerializable {

  case class hbaseRowFormat(columnFamily: String, qualifier: String, value: Any)

  val key: String

  def dataBindings: List[hbaseRowFormat]

  def toKeyValuePair: (ImmutableBytesWritable, Put) = {
    val p = new Put(Bytes.toBytes(key))
    dataBindings.foreach(row => p.add(
      Bytes.toBytes(row.columnFamily),
      Bytes.toBytes(row.qualifier),
      Bytes.toBytes(row.value.toString)))
    (new ImmutableBytesWritable, p)
  }
}