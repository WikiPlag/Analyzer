package de.htw.ai.wikiplag.textProcessing.indexer

import de.htw.ai.wikiplag.textProcessing.parser.WikiDumpParser
import de.htw.ai.wikiplag.textProcessing.Tokenizer

import scala.collection.immutable.HashMap

/** Generates the index for the Wikiplag-algorithm
  *
  * Use WikiplagIndex(Path) to generate the index for pages from "Path"
  *
  * Created by kuro on 10/30/16.
  */
object WikiplagIndex {
  type TokenMap = Map[String, List[(BigInt, List[Int])]]

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
    } yield (token, id, pos))
      // Builds Dictionary (TreeMap - Key: Token, Value: List of tuples (Text-ID, Position in Text)
      .foldLeft(new HashMap[String, HashMap[BigInt, List[Int]]]())((map, elem) =>
        map.updated(elem._1, map.get(elem._1) match {
          case Some(x) => x.updated(elem._2, elem._3 :: x.getOrElse(elem._2, List()))
          case _ => HashMap().updated(elem._2, List(elem._3))
        })).mapValues(_.toList)
  }
}