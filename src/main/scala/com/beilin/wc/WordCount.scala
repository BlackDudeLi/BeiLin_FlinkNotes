package com.beilin.wc

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

// 批处理的word count
object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建一个执行环境
    val env :ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // 从文件中读取数据
    val inputPath = "E:\\MY_Flink\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    val inputDS : DataSet[String] = env.readTextFile(inputPath)
    // 对数据进行转换处理，先分词，再按照word分组，最后进行聚合统计
    val wordCountDS :DataSet[(String,Int)]= inputDS
      .flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)  // 第一个元素为key分组
      .sum(1)       // 对所有数据的第二个元素求和

    // 打印输出
    wordCountDS.print()

  }

}
