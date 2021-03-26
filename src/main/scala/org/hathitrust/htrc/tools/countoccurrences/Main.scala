package org.hathitrust.htrc.tools.countoccurrences

import java.io.PrintWriter

import com.gilt.gfc.time.Timer
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.hathitrust.htrc.data.ops.TextOptions
import org.hathitrust.htrc.data.{HtrcVolume, HtrcVolumeId}
import org.hathitrust.htrc.tools.countoccurrences.Helper._
import org.hathitrust.htrc.tools.scala.io.IOUtils.using
import org.hathitrust.htrc.tools.spark.errorhandling.ErrorAccumulator
import org.hathitrust.htrc.tools.spark.errorhandling.RddExtensions._

import scala.io.{Codec, Source, StdIn}

/**
  * Given a set of terms and a set of HT volume IDs, this tool counts the number of times
  * these terms appear in the specified volumes, and outputs the result as CSV. The first
  * column of the CSV represents the volume ID, while the rest of the columns represent
  * the given search term counts. The (x,y) entry in the table represents the number of times
  * term 'y' was found in volume 'x'.
  *
  * Each term is defined through a regular expression for added flexibility. The terms are
  * read from a file provided by the user. Each line of the file contains two columns,
  * separated by a tab (\t) character. The first column is the term whose occurrence count
  * will be reported in the CSV results, and the second column contains the regular expression
  * that defines the search criteria for the term. All matches of the regular expression in
  * the text are counted, and the total count assigned to the term.
  *
  * @author Boris Capitanu
  */

object Main {
  val appName = "count-occurrences"

  def stopSparkAndExit(sc: SparkContext, exitCode: Int = 0): Unit = {
    try {
      sc.stop()
    }
    finally {
      System.exit(exitCode)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val numPartitions = conf.numPartitions.toOption
    val numCores = conf.numCores.map(_.toString).getOrElse("*")
    val pairtreeRootPath = conf.pairtreeRootPath().toString
    val keywordsPath = conf.keywordsPath()
    val outputPath = conf.outputPath().toString
    val htids = conf.htids.toOption match {
      case Some(file) => using(Source.fromFile(file))(_.getLines().toList)
      case None => Iterator.continually(StdIn.readLine()).takeWhile(_ != null).toSeq
    }

    // set up logging destination
    conf.sparkLog.toOption match {
      case Some(logFile) => System.setProperty("spark.logFile", logFile)
      case None =>
    }
    System.setProperty("logLevel", conf.logLevel().toUpperCase)

    val sparkConf = new SparkConf()
    sparkConf.setAppName(appName)
    sparkConf.setIfMissing("spark.master", s"local[$numCores]")
    val sparkMaster = sparkConf.get("spark.master")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    try {
      logger.info("Starting...")
      logger.info(s"Spark master: $sparkMaster")

      val t0 = Timer.nanoClock()

      // load the keywords and keyword regex patterns from the input file
      val keywordPatterns = using(Source.fromFile(keywordsPath)(Codec.UTF8)) { f =>
        f.getLines()
          .map(_.split("\t") match {
            case Array(name, pat) => name -> pat
          })
          .toArray
      }

      val keywords = keywordPatterns.map(_._1)
      val patterns = sc.broadcast(keywordPatterns.map(_._2))

      // set up the DataFrame schema where the first column is the volume id,
      // with the search terms making up the rest of the columns
      val idField = StructField("volid", StringType, nullable = false)
      val kwFields = keywords.map(StructField(_, IntegerType, nullable = false))
      val schema = StructType(Seq(idField) ++ kwFields)

      conf.outputPath().mkdirs()

      val idsRDD = numPartitions match {
        case Some(n) => sc.parallelize(htids, n) // split input into n partitions
        case None => sc.parallelize(htids) // use default number of partitions
      }

      // load the HTRC volumes from pairtree (and save any errors)
      val volumeErrAcc = new ErrorAccumulator[String, String](identity)(sc)
      val volumesRDD = idsRDD.tryMap { id =>
        val pairtreeVolume =
          HtrcVolumeId
            .parseUnclean(id)
            .map(_.toPairtreeDoc(pairtreeRootPath))
            .get

        HtrcVolume.from(pairtreeVolume)(Codec.UTF8).get
      }(volumeErrAcc)

      // ... and count the number of matches of the search terms in the volume text
      val errorsOccurrences = new ErrorAccumulator[HtrcVolume, String](_.volumeId.uncleanId)(sc)
      val occurrencesRDD = volumesRDD.tryMap(vol => {
        val text = vol.structuredPages.map(_.body(TextOptions.DehyphenateAtEol)).mkString
        val occurrences = countOccurrences(patterns.value, text)
        vol.volumeId.uncleanId -> occurrences
      })(errorsOccurrences)

      // convert the occurrences to Rows to be added to a DataFrame for saving to disk
      // skip any rows that did not match any of the searched regexes
      val rows = occurrencesRDD
        .collect {
          case (id, kwCounts) if kwCounts.exists(_ > 0) =>
            val rowData = Seq(id) ++ kwCounts
            Row(rowData: _*)
        }

      val kwCountsDF = spark.createDataFrame(rows, schema)

      // save the resulting DataFrame as TSV
      kwCountsDF.write
        .option("header", "false")
        .option("sep", "\t")
        .option("encoding", "UTF-8")
        .csv(outputPath + "/matches")

      using(new PrintWriter(outputPath + "/header.tsv"))(_.println(schema.fieldNames.mkString("\t")))

      if (volumeErrAcc.nonEmpty || errorsOccurrences.nonEmpty) {
        logger.info("Writing error report(s)...")
        if (volumeErrAcc.nonEmpty)
          volumeErrAcc.saveErrors(new Path(outputPath, "id_errors.txt"))
        if (errorsOccurrences.nonEmpty)
          errorsOccurrences.saveErrors(new Path(outputPath, "occurrences_errors.txt"))
      }

      val t1 = Timer.nanoClock()
      val elapsed = t1 - t0
      logger.info(f"All done in ${Timer.pretty(elapsed)}")
    }
    catch {
      case e: Throwable =>
        logger.error(s"Uncaught exception", e)
        stopSparkAndExit(sc, exitCode = 500)
    }

    stopSparkAndExit(sc)
  }

}
