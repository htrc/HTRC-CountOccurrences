package org.hathitrust.htrc.tools.countoccurrences

import org.apache.log4j.Logger

import scala.util.matching.Regex

object Executor {
  @transient lazy val logger = Logger.getLogger("CountOccurrences")

  val HyphenWordRegex = """(?m)(\S*\p{L})-\n(\p{L}\S*)\s*""".r

  def normalizeText(s: String): String = {
    var normText = s.split('\n').toList.map(_.trim).filterNot(_.isEmpty).mkString("\n")
    normText = HyphenWordRegex.replaceAllIn(normText, "$1$2\n")
    //normText = normText.replaceAll("""\n""", " ")
    //normText = normText.replaceAll("""([^\P{P}'-]+)""", " $1 ")
    normText
  }

  def countOccurrences(keywords: Seq[String], text: String): Seq[(String, Int)] = {
    val matchCounts = keywords
      .map(_.split("""\s+""").map(Regex.quote).mkString("""\b""", """\s""", """\b""").r)
      .map(_.findAllMatchIn(text).size)

    keywords.zip(matchCounts)
  }
}
