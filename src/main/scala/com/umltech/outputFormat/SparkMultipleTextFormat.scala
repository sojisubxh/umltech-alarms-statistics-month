package com.umltech.outputFormat

import com.umltech.MonthStatusApp
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapred.{FileOutputFormat, InvalidJobConfException, JobConf, RecordWriter}
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.hadoop.util.Progressable

/**
  *
  * <p>
  * ----------------------------------------------------------------------------- <br>
  * 工程名 ：umltech-alarms-statistics-month <br>
  * 功能：<br>
  * 描述：<br>
  * 授权 : (C) Copyright (c) 2016<br>
  * 公司 : 北京博创联动科技有限公司<br>
  * ----------------------------------------------------------------------------- <br>
  * 修改历史<br>
  * <table width="432" border="1">
  * <tr><td>版本</td><td>时间</td><td>作者</td><td>改变</td></tr>
  * <tr><td>1.0</td><td>2019/3/20</td><td>xuehui</td><td>创建</td></tr>
  * </table>
  * <br>
  * <font color="#FF0000">注意: 本内容仅限于[北京博创联动科技有限公司]内部使用，禁止转发</font><br>
  *
  * @version 1.0
  * @author xuehui
  * @since JDK1.8
  */
class SparkMultipleTextFormat extends MultipleTextOutputFormat[String, String] {

    override def generateFileNameForKeyValue(key: String, value: String, name: String): String = name

    override def getBaseRecordWriter(fs: FileSystem, job: JobConf, name: String, arg3: Progressable): RecordWriter[String, String] = {
        super.getBaseRecordWriter(fs, job, job.get(MonthStatusApp.PREFIX), arg3)
    }

    override def checkOutputSpecs(ignored: FileSystem, job: JobConf): Unit = {
        var outDir = FileOutputFormat.getOutputPath(job)
        if (outDir == null && job.getNumReduceTasks != 0) throw new InvalidJobConfException("Output directory not set in JobConf.")
        if (outDir != null) {
            val fs = outDir.getFileSystem(job)
            // normalize the output directory
            outDir = fs.makeQualified(outDir)
            FileOutputFormat.setOutputPath(job, outDir)
            // get delegation token for the outDir's file system
            TokenCache.obtainTokensForNamenodes(job.getCredentials, Array[Path](outDir), job)
        }
    }
}
