package org.hathitrust.htrc.tools.countoccurrences

import java.io.File

import com.gilt.gfc.time.Timer
import edu.illinois.i3.scala.utils.metrics.Timer.time
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.hathitrust.htrc.tools.countoccurrences.Executor._
import org.hathitrust.htrc.tools.pairtreetotext.PairtreeToText.pairtreeToText
import org.rogach.scallop.ScallopConf

import scala.io.{Codec, Source, StdIn}
import scala.util.Failure

object Main {

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val pairtreeRootPath = conf.pairtreeRootPath()
    val keywordsPath = conf.keywordsPath()
    val outputPath = conf.outputPath()
    val numPartitions = conf.numPartitions.toOption
    val htids = conf.htids.toOption match {
      case Some(file) => Source.fromFile(file).getLines().toSeq
      case None => Iterator.continually(StdIn.readLine()).takeWhile(_ != null).toSeq
    }

    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local[*]")
    sparkConf.setIfMissing("spark.app.name", "count-occurrences")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext

    val (_, elapsed) = time {
      val keywords = Source.fromFile(keywordsPath)(Codec.UTF8).getLines().toSeq
      val idField = StructField("volid", StringType, nullable = false)
      val kwFields = keywords.map(StructField(_, IntegerType, nullable = false))
      val schema = StructType(Seq(idField) ++ kwFields)

      val ids = numPartitions match {
        case Some(n) => sc.parallelize(htids, n)
        case None => sc.parallelize(htids)
      }

      ids.cache()

      val texts = ids.map(pairtreeToText(_, pairtreeRootPath)(Codec.UTF8).map(_._2))
      val normalizedTexts = texts.map(_.map(normalizeText))
      val occurrences = normalizedTexts.map(_.map(countOccurrences(keywords, _)))

      val results = ids.zip(occurrences)

      results.cache()

      val rows = results
        .filter(_._2.isSuccess)
        .map {
          case (id, kwCounts) => Seq(id) ++ kwCounts.get.map(_._2)
        }
        .map(Row(_: _*))

      val kwCountsDF = spark.createDataFrame(rows, schema)

      kwCountsDF.write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .save(outputPath.toString)

      results.filter(_._2.isFailure).collect().foreach {
        case (id, Failure(err)) => logger.error(s"Error [$id]: ${err.getMessage}")
        case _ =>
      }
    }

    logger.info(f"All done in ${Timer.pretty(elapsed*1e6.toLong)}")
  }

}

/**
  * Command line argument configuration
  *
  * @param arguments The cmd line args
  */
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val (appTitle, appVersion, appVendor) = {
    val p = getClass.getPackage
    val nameOpt = Option(p).flatMap(p => Option(p.getImplementationTitle))
    val versionOpt = Option(p).flatMap(p => Option(p.getImplementationVersion))
    val vendorOpt = Option(p).flatMap(p => Option(p.getImplementationVendor))
    (nameOpt, versionOpt, vendorOpt)
  }

  version(appTitle.flatMap(
    name => appVersion.flatMap(
      version => appVendor.map(
        vendor => s"$name $version\n$vendor"))).getOrElse("htrc-countoccurrences"))

  val numPartitions = opt[Int]("num-partitions",
    descr = "The number of partitions to split the input set of HT IDs into, " +
      "for increased parallelism",
    required = false,
    argName = "N",
    validate = 0<
  )

  val pairtreeRootPath = opt[File]("pairtree",
    descr = "The path to the paitree root hierarchy to process",
    required = true,
    argName = "DIR"
  )

  val keywordsPath = opt[File]("keywords",
    descr = "The path to the file containing the keywords (one per line) to count",
    required = true,
    argName = "FILE"
  )

  val outputPath = opt[File]("output",
    descr = "Write the output to DIR",
    required = true,
    argName = "DIR"
  )

  val htids = trailArg[File]("htids",
    descr = "The file containing the HT IDs to be searched (if not provided, will read from stdin)",
    required = false
  )

  validateFileExists(pairtreeRootPath)
  validateFileExists(keywordsPath)
  validateFileExists(htids)
  verify()
}