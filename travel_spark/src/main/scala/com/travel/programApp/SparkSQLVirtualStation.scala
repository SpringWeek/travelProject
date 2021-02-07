package com.travel.programApp

import com.travel.common.{Constants, District}
import com.travel.utils.{HbaseTools, SparkUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.locationtech.jts.geom.{Point, Polygon}
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable

object SparkSQLVirtualStation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setMaster("local[4]").setAppName("sparkHbase")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //设置日志级别，避免出现太多日志信息
    sparkSession.sparkContext.setLogLevel("WARN")
    //hbase配置
    val hconf: Configuration = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "node01,node02,node03")
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    hconf.setInt("hbase.client.operation.timeout", 3000)
    val hbaseFrame: DataFrame = HbaseTools.loadHBaseData(sparkSession, hconf)

    hbaseFrame.show(20)

    //将dataFrame注册成为一张表
    hbaseFrame.createOrReplaceTempView("order_df")
    //获取虚拟车站，每个虚拟车站里面所有的经纬度坐标点只取一个最小的
    val virtual_rdd: RDD[Row] = SparkUtils.getVirtualFrame(sparkSession)

    virtual_rdd.foreach(eachLine => {
      val str: String = eachLine.getString(0)
      println(str)
    })


    //广播每个区域的经纬度边界
    val districtsBroadcastVar: Broadcast[java.util.ArrayList[District]] = SparkUtils.broadCastDistrictValue(sparkSession)
    //将每个区域的边界转换成为一个多边形，使用Polygon这个对象来表示，返回一个元组（每一个区域封装对象District，多边形Polygon）
    //判断每个虚拟车站，是属于哪一个区里面的
    val finalSaveRow: RDD[mutable.Buffer[Row]] = virtual_rdd.mapPartitions(eachPartition => {
      //使用JTS-Tools来通过多个经纬度，画出多边形
      import org.geotools.geometry.jts.JTSFactoryFinder
      val geometryFactory = JTSFactoryFinder.getGeometryFactory(null)
      var reader = new WKTReader(geometryFactory)
      //将哪一个区的，哪一个边界求出来
      val wktPolygons: mutable.Buffer[(District, Polygon)] = SparkUtils.changeDistictToPolygon(districtsBroadcastVar, reader)
      eachPartition.map(row => {
        val lng = row.getAs[String]("starting_lng")
        val lat = row.getAs[String]("starting_lat")
        val wktPoint = "POINT(" + lng + " " + lat + ")";
        val point: Point = reader.read(wktPoint).asInstanceOf[Point];

        val rows: mutable.Buffer[Row] = wktPolygons.map(polygon => {
          if (polygon._2.contains(point)) {
            val fields = row.toSeq.toArray ++ Seq(polygon._1.getName)
            Row.fromSeq(fields)
          } else {
            null
          }
        }).filter(null != _)
        rows
      })
    })
    //将我们的数据压平，然后转换成为DF
    val rowRdd: RDD[Row] = finalSaveRow.flatMap(x => x)
    //将数据保存到HBase里面去
    HbaseTools.saveOrWriteData(hconf, rowRdd, Constants.VIRTUAL_STATION)
  }
}
