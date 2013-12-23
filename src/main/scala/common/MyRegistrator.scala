package common

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import job.bouygues.entity._

/**
 * Created with IntelliJ IDEA.
 * User: coderh
 * Date: 12/16/13
 * Time: 3:15 PM
 */

class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    //    kryo.setRegistrationRequired(true)
    kryo.register(classOf[org.apache.hadoop.hbase.client.Result])
    kryo.register(classOf[BygClient])
    kryo.register(classOf[Visit])
    kryo.register(classOf[View])
  }
}