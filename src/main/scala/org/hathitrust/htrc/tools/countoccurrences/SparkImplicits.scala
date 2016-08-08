package org.hathitrust.htrc.tools.countoccurrences

import java.nio.charset.Charset
import java.nio.file.StandardOpenOption._
import java.nio.file.{Files, Paths}

import org.apache.spark.rdd.RDD
import resource._

import scala.reflect.ClassTag

object SparkImplicits {

  implicit class RDDEx[T:ClassTag](rdd: RDD[T]) {

    def filterOutAndSave(pred: T => Boolean,
                         saveDir: String,
                         charset: String = "UTF-8",
                         tos: (T) => String = (x:T) => x.toString): RDD[T] = {
      Files.createDirectories(Paths.get(saveDir))
      rdd.mapPartitionsWithIndex((i, p) => p.partition(pred) match {
        case (s, f) =>
          val savePath = Paths.get(saveDir, f"part-$i%05d")
          for (writer <- managed(Files.newBufferedWriter(savePath, Charset.forName(charset), CREATE_NEW))) {
            writer.write(s.map(tos(_) + "\n").mkString)
          }
          f
      }, preservesPartitioning = true)
    }

  }

  import org.apache.hadoop.io.NullWritable
  import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

  class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
    override def generateActualKey(key: Any, value: Any): Any =
      NullWritable.get()

    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
      key.asInstanceOf[String]
  }

// USAGE of RDDMultipleTextOutputFormat
//
//  object Split {
//    def main(args: Array[String]) {
//      val conf = new SparkConf().setAppName("Split" + args(1))
//      val sc = new SparkContext(conf)
//      sc.textFile("input/path")
//        .map(a => (k, v)) // Your own implementation
//        .partitionBy(new HashPartitioner(num))
//        .saveAsHadoopFile("output/path", classOf[String], classOf[String],
//          classOf[RDDMultipleTextOutputFormat])
//      apache.spark.stop()
//    }
//  }

}
