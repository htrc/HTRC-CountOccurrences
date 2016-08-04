package org.hathitrust.htrc.tools.countoccurrences

import scala.util.matching.Regex

object Executor {
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
      .map(_.split("""\s+""").map(Regex.quote).mkString("""\s"""))
      .map(k => s"""\b$k\b""".r)
      .map(_.findAllMatchIn(text).size)

    keywords.zip(matchCounts)
  }
}
