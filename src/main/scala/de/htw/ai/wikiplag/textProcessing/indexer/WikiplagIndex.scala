package de.htw.ai.wikiplag.textProcessing.indexer

import de.htw.ai.wikiplag.textProcessing.parser.WikiDumpParser

import scala.collection.immutable.TreeMap

/** Generates the Index for the Wikiplag-algorithm
  *
  * Use WikiplagIndex() to generate the Index
  *
  * Created by kuro on 10/30/16.
  */
object WikiplagIndex {
  /** Dump for building */
  private def dumpTokenizer(text: String, id: BigInt): (BigInt, List[String]) = (id, text.split(" ").toList)

  type TokenMap = TreeMap[String, List[(BigInt, Int)]]
  type Text = (String, BigInt)

  /** Generates the WikiplagIndex
    *
    * @return WikiplagIndex
    */
  def apply(): TokenMap = {
    (for {
      // Groups the incoming stream into chunks for processing
      articles <- WikiDumpParser.generateWikiArticleList().grouped(50)
      // Processes each element of the group
      (text, id) <- articles
      // Generates the tokens and the position of that token
      (token, pos) <- WikiplagIndex.dumpTokenizer(text, id)._2.zipWithIndex
      // Builds one element (Token, (Text-ID, Position in Text))
    } yield (token,(id, pos)))
      // Builds Dictionary (TreeMap - Key: Token, Value: List of tuples (Text-ID, Position in Text)
      .foldLeft(new TokenMap())((tmap, elem) => tmap.updated(elem._1, tmap.getOrElse(elem._1, List()) :+ elem._2))
  }
}