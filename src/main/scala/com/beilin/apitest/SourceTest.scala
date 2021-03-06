package com.beilin.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
// 定义样例类，温度传感器，
case class SensorReading(id:String,timestamp:Long,temperature:Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 1.从集合中读取数据
    val dataList = List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    )
    val stream1 = env.fromCollection(dataList)

    // 2. 从文件中读取数据
    val inputPath = "E:\\Project\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val stream2 = env.readTextFile(inputPath)

    // 3. 从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.server","bigdata01:9092")
    properties.setProperty("group.id", "consumer-group")


    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))

    stream1.print()
    stream2.print()
    // stream3.print()
    env.execute("source test")
  }

}
