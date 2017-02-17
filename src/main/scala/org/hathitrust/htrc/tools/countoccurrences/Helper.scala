package org.hathitrust.htrc.tools.countoccurrences

import org.slf4j.{Logger, LoggerFactory}

object Helper {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(Main.appName)

  /**
    * Counts the number of matches of the given keywords in the given text
    *
    * @param patterns The regex patterns
    * @param text     The text
    * @return The match counts
    */
  def countOccurrences(patterns: Seq[String], text: String): Seq[Int] = {
    val matchCounts = patterns
      .map(_.r)
      .map(_.findAllMatchIn(text).size)

    matchCounts
  }
}
