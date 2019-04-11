package com.umltech

import java.util.Properties

import cn.hutool.core.util.StrUtil
import cn.hutool.setting.dialect.Props
import com.alibaba.fastjson.{JSON, JSONObject}
import com.umltech.outputFormat.SparkMultipleTextFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by xuehui on 2019/3/25.
  */
object MonthStatusApp {
    val props = new Props("application.properties")
    val PREFIX = "prefix"

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf()
        val jobConf = new JobConf()

        var inputDir = "" //输入文件目录
        var outputDir = "" //输出文件目录
        var month = ""

        //监测参数
        if (args == null || args.length == 0) {
            sparkConf.setAppName(this.getClass.getSimpleName)
            sparkConf.setMaster("local[*]")
            inputDir = "hdfs://cdh2:8020/spark/alarmStatus/day/2019/03/*"
            outputDir = "hdfs://cdh-master2:8020/data/spark/alarmStatus/month/2019/"
            month = "201903"
        } else {
            inputDir = args(0) //输入文件目录
            outputDir = args(1) //输出文件目录
            month = args(2)
        }

        //mysql config
        val mysqlTableName = props.getProperty("mysql.table.prefix") + month
        val url = getUrl(props.getProperty("mysql.ip"), props.getProperty("mysql.port"), props.getProperty("mysql.database"), props.getProperty("mysql.user"), props.getProperty("mysql.password"))
        val mysqlConf = new Properties
        mysqlConf.setProperty("driver", props.getProperty("mysql.driver"))
        mysqlConf.setProperty("batchsize", "5000")

        val sc = new SparkContext(sparkConf)
        val sqlContext = new SQLContext(sc)

        //当月所有日统计数据
        val inputRdd = sc.textFile(inputDir)

        val pairRdd = inputRdd.mapPartitions(iterator => {
            //Scala中ListBuffer类型本可以在添加元素时避免中间变量的产生，省空间，避免频繁的GC
            var list = ListBuffer[(String, String)]()

            iterator.foreach(data => {
                if (StrUtil.isNotBlank(data)) {
                    val json = JSON.parseObject(data)
                    json.remove("alarmList") //清除报警列表信息
                    list.+=:(json.getString("did") + "_" + json.getString("code"), json.toJSONString)
                }
            })
            list.toIterator
        }, preservesPartitioning = false)
          .reduceByKey((x, y) => {
              val json1: JSONObject = JSON.parseObject(x)
              val json2: JSONObject = JSON.parseObject(y)

              val updateTime1 = json1.getDate("updateTime")
              val updateTime2 = json2.getDate("updateTime")

              if (updateTime1.after(updateTime2)) {
                  json1.replace("total", String.valueOf(json1.getLong("total") + json2.getLong("total")))
                  json1.toJSONString
              } else {
                  json2.replace("total", String.valueOf(json1.getLong("total") + json2.getLong("total")))
                  json2.toJSONString
              }
          }).persist(StorageLevel.MEMORY_ONLY_SER)

        val resultRdd = pairRdd.mapPartitions(iter => {
            val res = ListBuffer[(String, String)]()
            iter.foreach {
                case ((did, value)) => res.+=:(null, value)
            }
            res.toIterator
        })

        //根据时间将数据写入到不同的hdfs目录中
        jobConf.set(PREFIX, month)
        resultRdd.repartition(1).saveAsHadoopFile(outputDir, classOf[String], classOf[String], classOf[SparkMultipleTextFormat], jobConf)
    }

    private def getUrl(ip: String, port: String, database: String, user: String, password: String): String = {
        new StringBuilder("jdbc:mysql://").append(ip).append(":").append(port).append("/").append(database).append("?user=").append(user).append("&password=").append(password).toString
    }
}
