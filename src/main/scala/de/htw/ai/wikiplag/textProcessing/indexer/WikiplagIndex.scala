package de.htw.ai.wikiplag.textProcessing.indexer

import de.htw.ai.wikiplag.textProcessing.parser.WikiDumpParser
import de.htw.ai.wikiplag.textProcessing.Tokenizer

import scala.collection.immutable.TreeMap

/** Generates the index for the Wikiplag-algorithm
  *
  * Use WikiplagIndex(Path) to generate the index for pages from "Path"
  *
  * Created by kuro on 10/30/16.
  */
object WikiplagIndex {
  type TokenMap = TreeMap[String, List[(BigInt, Int)]]
  type Text = (String, BigInt)

  /** Generates the WikiplagIndex
    *
    * @param path Path to WikiPages
    * @return WikiplagIndex
    */
  def apply(path: String): TokenMap = {
    (for {
      // Groups the incoming stream into chunks for processing
      articles <- WikiDumpParser.generateWikiArticleList(path).grouped(50)
      // Processes each element of the group
      (text, id) <- articles
      // Generates the tokens and the position of that token
      (token, pos) <- Tokenizer.tokenize(text).zipWithIndex
      // Builds one element (Token, (Text-ID, Position in Text))
    } yield (token,(id, pos)))
      // Builds Dictionary (TreeMap - Key: Token, Value: List of tuples (Text-ID, Position in Text)
      .foldLeft(new TokenMap())((tmap, elem) => tmap.updated(elem._1, tmap.getOrElse(elem._1, List()) :+ elem._2))
  }
}