package com.travel.utils

import com.typesafe.config.ConfigFactory

/**
 * Created by laowang
 */
class GlobalConfigUtils {
  val getProp = (argv: String) => conf.getString(argv)

  def heartColumnFamily = "MM" //conf.getString("heart.table.columnFamily")

  private def conf = ConfigFactory.load()

}

object GlobalConfigUtils extends GlobalConfigUtils {

  def main(args: Array[String]): Unit = {
    val str = getProp("spark.worker.timeout")
    println(str)
  }


}







