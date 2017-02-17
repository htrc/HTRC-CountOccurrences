package org.hathitrust.htrc.tools.countoccurrences

import java.io.File

import com.gilt.gfc.time.Timer
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.hathitrust.htrc.tools.countoccurrences.Helper._
import org.hathitrust.htrc.tools.pairtreetotext.{HTRCVolume, TextOptions}
import org.hathitrust.htrc.tools.spark.errorhandling.ErrorAccumulator
import org.hathitrust.htrc.tools.spark.errorhandling.RddExtensions._
import org.rogach.scallop.{ScallopConf, ScallopOption}

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

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val pairtreeRootPath = conf.pairtreeRootPath().toString
    val keywordsPath = conf.keywordsPath()
    val outputPath = conf.outputPath().toString
    val numPartitions = conf.numPartitions.toOption
    val htids = conf.htids.toOption match {
      case Some(file) => Source.fromFile(file).getLines().toSeq
      case None => Iterator.continually(StdIn.readLine()).takeWhile(_ != null).toSeq
    }

    conf.outputPath().mkdirs()

    // set up logging destination
    conf.sparkLog.toOption match {
      case Some(logFile) => System.setProperty("spark.logFile", logFile)
      case None =>
    }
    System.setProperty("logLevel", conf.logLevel().toUpperCase)

    // set up Spark context
    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local[*]")
    sparkConf.setIfMissing("spark.app.name", appName)

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext

    logger.info("Starting...")

    // record start time
    val t0 = System.nanoTime()

    // load the keywords and keyword regex patterns from the input file
    val keywordPatterns = Source.fromFile(keywordsPath)(Codec.UTF8).getLines()
      .map(_.split("\t") match {
        case Array(name, pat) => name -> pat
      }).toArray

    val keywords = keywordPatterns.map(_._1)
    val patterns = sc.broadcast(keywordPatterns.map(_._2))

    // set up the DataFrame schema where the first column is the volume id,
    // with the search terms making up the rest of the columns
    val idField = StructField("volid", StringType, nullable = false)
    val kwFields = keywords.map(StructField(_, IntegerType, nullable = false))
    val schema = StructType(Seq(idField) ++ kwFields)

    val idsRDD = numPartitions match {
      case Some(n) => sc.parallelize(htids, n)
      case None => sc.parallelize(htids)
    }

    // load the HTRC volumes from pairtree (and save any errors)
    val errorsVol = new ErrorAccumulator[String, String](identity)(sc)
    val htrcDocsRDD = idsRDD.tryMap(HTRCVolume(_, pairtreeRootPath)(Codec.UTF8))(errorsVol)

    // ... and count the number of matches of the search terms in the volume text
    val errorsOccurrences = new ErrorAccumulator[HTRCVolume, String](_.id)(sc)
    val occurrencesRDD = htrcDocsRDD.tryMap(doc => {
      val text = doc.getText(TextOptions.BodyOnly, TextOptions.FixHyphenation)
      val occurrences = countOccurrences(patterns.value, text)
      doc.id -> occurrences
    })(errorsOccurrences)

    // convert the occurrences to Rows to be added to a DataFrame for saving to disk
    val rows = occurrencesRDD.map {
      case (id, kwCounts) =>
        val rowData = Seq(id) ++ kwCounts
        Row(rowData: _*)
    }

    val kwCountsDF = spark.createDataFrame(rows, schema)

    // save the resulting DataFrame as CSV
    kwCountsDF.write
      .option("header", "true")
      .csv(outputPath + "/matches")

    if (errorsVol.nonEmpty || errorsOccurrences.nonEmpty)
      logger.info("Writing error report(s)...")

    // save any errors to the output folder
    if (errorsVol.nonEmpty)
      errorsVol.saveErrors(new Path(outputPath, "volume_errors.txt"), _.toString)

    if (errorsOccurrences.nonEmpty)
      errorsOccurrences.saveErrors(new Path(outputPath, "occurrences_errors.txt"), _.toString)

    // record elapsed time and report it
    val t1 = System.nanoTime()
    val elapsed = t1 - t0

    logger.info(f"All done in ${Timer.pretty(elapsed)}")
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

  version(appVersion.flatMap(
    version => appVendor.map(
      vendor => s"${Main.appName} $version\n$vendor")).getOrElse(Main.appName))

  val sparkLog: ScallopOption[String] = opt[String]("spark-log",
    descr = "Where to write logging output from Spark to",
    argName = "FILE",
    noshort = true
  )

  val logLevel: ScallopOption[String] = opt[String]("log-level",
    descr = "The application log level; one of INFO, DEBUG, OFF",
    argName = "LEVEL",
    default = Some("INFO"),
    validate = level => Set("INFO", "DEBUG", "OFF").contains(level.toUpperCase)
  )

  val numPartitions: ScallopOption[Int] = opt[Int]("num-partitions",
    descr = "The number of partitions to split the input set of HT IDs into, " +
      "for increased parallelism",
    required = false,
    argName = "N",
    validate = 0 <
  )

  val pairtreeRootPath: ScallopOption[File] = opt[File]("pairtree",
    descr = "The path to the paitree root hierarchy to process",
    required = true,
    argName = "DIR"
  )

  val keywordsPath: ScallopOption[File] = opt[File]("keywords",
    descr = "The path to the file containing the keyword patterns (one per line) to count",
    required = true,
    argName = "FILE"
  )

  val outputPath: ScallopOption[File] = opt[File]("output",
    descr = "Write the output to DIR",
    required = true,
    argName = "DIR"
  )

  val htids: ScallopOption[File] = trailArg[File]("htids",
    descr = "The file containing the HT IDs to be searched (if not provided, will read from stdin)",
    required = false
  )

  validateFileExists(pairtreeRootPath)
  validateFileExists(keywordsPath)
  validateFileExists(htids)
  verify()
}