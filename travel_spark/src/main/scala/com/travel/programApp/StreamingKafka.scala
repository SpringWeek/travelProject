package com.travel.programApp

import com.travel.common.{ConfigUtil, Constants, HBaseUtil, JedisUtil}
import com.travel.loggings.Logging
import com.travel.utils.HbaseTools
import org.apache.hadoop.hbase.client.{Admin, ColumnFamilyDescriptor, ColumnFamilyDescriptorBuilder, Connection, TableDescriptor, TableDescriptorBuilder}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object StreamingKafka extends Logging {

  def main(args: Array[String]): Unit = {
    val brokers = ConfigUtil.getConfig(Constants.KAFKA_BOOTSTRAP_SERVERS)
    val topics = Array(ConfigUtil.getConfig(Constants.CHENG_DU_GPS_TOPIC), ConfigUtil.getConfig(Constants.HAI_KOU_GPS_TOPIC))
    val conf = new SparkConf().setMaster("local[1]").setAppName("sparkKafka")
    val group: String = "gps_consum_group"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest", // earliest,latest,和none
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val context: SparkContext = sparkSession.sparkContext
    context.setLogLevel("WARN")
    // val streamingContext = new StreamingContext(conf,Seconds(5))
    //获取streamingContext
    val streamingContext: StreamingContext = new StreamingContext(context, Seconds(1))
    val result: InputDStream[ConsumerRecord[String, String]] = HbaseTools.getStreamingContextFromHBase(streamingContext, kafkaParams, topics, group, "(.*)gps_topic")

    /**
     * 将数据保存到HBase当中去，以及将成都的数据，保存到redis里面去
     */
    result.foreachRDD(eachRdd => {
      if (!eachRdd.isEmpty()) {
        eachRdd.foreachPartition(eachPartition => {
          val connection: Connection = HBaseUtil.getConnection
          val jedis: Jedis = JedisUtil.getJedis
          //判断表是否存在，如果不存在就进行创建
          val admin: Admin = connection.getAdmin
          if (!admin.tableExists(TableName.valueOf(Constants.HTAB_GPS))) {
            val tableNameDescriptorBuilder: TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(Constants.HTAB_GPS))
            val columnFamilyDescriptor: ColumnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Constants.DEFAULT_FAMILY.getBytes()).build()
            val tableDescriptor: TableDescriptor = tableNameDescriptorBuilder.setColumnFamily(columnFamilyDescriptor).build()
            admin.createTable(tableDescriptor)
          }
          if (!admin.tableExists(TableName.valueOf(Constants.HTAB_HAIKOU_ORDER))) {
            val tableNameDescriptorBuilder: TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(Constants.HTAB_HAIKOU_ORDER))
            val columnFamilyDescriptor: ColumnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Constants.DEFAULT_FAMILY.getBytes()).build()
            val tableDescriptor: TableDescriptor = tableNameDescriptorBuilder.setColumnFamily(columnFamilyDescriptor).build()
            admin.createTable(tableDescriptor)
          }
          eachPartition.foreach(record => {
            //保存到HBase和redis
            val consumerRecords: ConsumerRecord[String, String] = HbaseTools.saveToHBaseAndRedis(connection, jedis, record)
          })
          JedisUtil.returnJedis(jedis)
          connection.close()
        })

        //更新offset
        val offsetRanges: Array[OffsetRange] = eachRdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //result.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)  //将offset提交到默认的kafka的topic里面去保存
        for (eachrange <- offsetRanges) {
          val startOffset: Long = eachrange.fromOffset //起始offset
          val endOffset: Long = eachrange.untilOffset //结束offset
          val topic: String = eachrange.topic
          val partition: Int = eachrange.partition
          HbaseTools.saveBatchOffset(group, topic, partition + "", endOffset)
        }
      }
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}


