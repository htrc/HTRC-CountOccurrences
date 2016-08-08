package org.hathitrust.htrc.tools.countoccurrences

import org.apache.log4j.Logger

import scala.util.matching.Regex

object Executor {
  @transient lazy val logger = Logger.getLogger("CountOccurrences")

  val HyphenWordRegex = """(?m)(\S*\p{L})-\n(\p{L}\S*)\s*""".r

  /**
    * Cleans up and normalizes the given text by joining hyphenated words occurring
    * at the end of the line and removing the empty lines
    *
    * @param s The text
    * @return The normalized text
    */
  def normalizeText(s: String): String = {
    var normText = s.split('\n').toList.map(_.trim).filterNot(_.isEmpty).mkString("\n")
    normText = HyphenWordRegex.replaceAllIn(normText, "$1$2\n")
    //normText = normText.replaceAll("""\n""", " ")
    //normText = normText.replaceAll("""([^\P{P}'-]+)""", " $1 ")
    normText
  }

  /**
    * Counts the number of matches of the given keywords in the given text
    *
    * @param keywords The keywords/terms
    * @param text The text
    * @return A sequence of tuples containing (keyword, matchCount)
    */
  def countOccurrences(keywords: Seq[String], text: String): Seq[(String, Int)] = {
    val matchCounts = keywords
      .map(_.split("""\s+""").map(Regex.quote).mkString("""\b""", """\s""", """\b""").r)
      .map(_.findAllMatchIn(text).size)

    keywords.zip(matchCounts)
  }
}
