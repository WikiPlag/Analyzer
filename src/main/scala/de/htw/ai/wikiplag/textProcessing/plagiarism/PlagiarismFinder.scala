package de.htw.ai.wikiplag.textProcessing.plagiarism


import de.htw.ai.wikiplag.data.{InverseIndexBuilderImpl, MongoDbClient}
import org.apache.spark.SparkContext

/**
  * Repr. all hyperparameter of our model
  *
  * @param textSplitLength           Number of words the text will be splitted into
  * @param textSplitStep             Offset of words the parts will be apart of
  * @param matchingWords             Threshold of matching words per article in percent [0-1]
  * @param maxDistanceAlpha          Maximal distance alpha between each group of words
  * @param maxDistanceBeta           Maximal distance beta between each group of words
  * @param minGroupSize              Minimal word group size
  */
class Hyper(val textSplitLength: Int = 20, val textSplitStep: Int = 15,
            val matchingWords: Double = 0.70, val maxDistanceAlpha: Int = 3, val maxDistanceBeta: Int = 7,
            val minGroupSize: Int = 10) extends Serializable {}

/**
  * Repr. the start of an potential plagiarism
  *
  * @param positionInputText         Position of the beginning in the input text
  * @param documentID                ID of the article
  * @param positionArticle           Position of the beginning in the article
  */
class Start(val positionInputText: PlagiarismFinder.InPos, val documentID: PlagiarismFinder.ID,
            val positionArticle: PlagiarismFinder.ArtPos) extends Serializable {
  override def toString: String = s"[InText: $positionInputText, DocID: $documentID, WikiText: $positionArticle]"
}

/**
  * Repr. the end of an potential plagiarism
  *
  * @param positionInputText         Position of the end in the input text
  * @param documentID                ID of the article
  * @param positionArticle           Position of the end in the article
  */
class End(val positionInputText: PlagiarismFinder.InPos, val documentID: PlagiarismFinder.ID,
          val positionArticle: PlagiarismFinder.ArtPos) extends Serializable {
  override def toString: String = s"[InText: $positionInputText, DocID: $documentID, WikiText: $positionArticle]"
}

/**
  * Repr. one potential plagiarism
  *
  * @param start                      Start of the plagiarism
  * @param end                        End of the plagiarism
  * @param score                      Score of the plagiarism [experimental]
  */
class Match(val start: Start, val end: End, val score: Double) {
  override def toString: String = s"[Start:\t $start \n End:\t $end \n\t Score: [$score]]"
}

/**
  * Created by Rob & Kuro on 02.11.2016 last update 18.02.2017.
  */
object PlagiarismFinder extends Serializable {
  type InPos = Int
  type ID = Long
  type ArtPos = Int
  type Delta = Int

  // Number of chunks to process
  var nChunks: Int = _

  /**
    * Starts the PlagiarismFinder.
    *
    * @param sc               Spark context
    * @param textSplitLength  Number of words the text will be splitted into
    * @param textSplitStep    Offset of words the parts will be apart of
    * @param matchingWords    Threshold of matching words per article in percent [0-1]
    * @param maxDistanceAlpha Maximal distance alpha between each group of words
    * @param maxDistanceBeta  Maximal distance beta between each group of words
    * @param minGroupSize     Minimal word group size
    * @return                 List of Matches
    */
  def apply(sc: SparkContext, inputText: String, textSplitLength: Int = 20, textSplitStep: Int = 15,
            matchingWords: Double = 0.70, maxDistanceAlpha: Int = 3, maxDistanceBeta: Int = 7,
            minGroupSize: Int = 10): Map[ID, List[Match]] = {
    val client = MongoDbClient(sc, "hadoop03.f4.htw-berlin.de", 27020, "wikiplag", "wikiplag", "Ku7WhY34")
    MongoDbClient.open()

    // Wraps the hyperparameter in an object for better handling
    val hyper = new Hyper(textSplitLength, textSplitStep, matchingWords, maxDistanceAlpha, maxDistanceBeta, minGroupSize)

    // #################################################################################################################
    // ################################################## Preparation ##################################################
    // #################################################################################################################

    // Splitting the text into chunks and remember the chunk position
    println("Start splitting Text ...")
    val textParts = PlagiarismFinder.splitText(inputText, hyper).zipWithIndex

    // For display the progress
    nChunks = textParts.size

    // Fetch the inverse index
    val indices = textParts.map { case ((chunk, _), _) => client.getInvIndexRDD(chunk.toSet) }
    println("Done splitting Text ...")

    // #################################################################################################################
    // #################################################### Process ####################################################
    // #################################################################################################################

    // Process each part and find potential plagiarisms
    val processed = textParts.flatMap {
      case c@((_, _), chunkID) => PlagiarismFinder.checkForPlagiarism(indices(chunkID).collect.toList, c, hyper)
    }

    // #################################################################################################################
    // ##################################################### Clean #####################################################
    // #################################################################################################################

    // Clean the output
    val cleaned: List[(ID, List[(InPos, (ArtPos, ArtPos, Double), Delta)])] =
      processed.groupBy[PlagiarismFinder.ID](_._2).mapValues(artGr => {

        val artGrSorted = artGr.sortBy[PlagiarismFinder.ArtPos](_._3._1)
        val (headInPos, headID, headArt) = artGrSorted.head

        (headID, (headInPos, headArt, 0)) :: artGrSorted.zip(artGrSorted.tail).map {
          case (((_, _, (_, lArtPosEnd, _)), (inPos, id, artPos@(rArtPosStart, _, _)))) =>
            (id, (inPos, artPos, rArtPosStart - lArtPosEnd))
        }

      }).toList.map { case ((id, group)) => (id, group.map { case ((_, part)) => part }) }

    val regions = PlagiarismFinder.splitIntoRegions(cleaned, new Hyper(maxDistanceBeta = 50, minGroupSize = 0))
    val pointer = PlagiarismFinder.getPointerToRegions(regions, hyper)
    pointer
      .map { case (((sIn, sID, sArt), (eIn, eID, eArt), score)) =>
        new Match(new Start(sIn, sID, sArt), new End(eIn, eID, eArt), score)
      }
      .groupBy(_.start.documentID)
      .mapValues(_.sortBy(_.start.positionInputText))
  }

  /**
    * Returns the tokens for a text.
    * The text gets tokenized.
    * Then its sliced into equal parts which are overlapping.
    *
    * @param text  Text to process
    * @param hyper Hyperparameter
    * @return      Lists with equal number of tokens per slice
    */
  def splitText(text: String, hyper: Hyper): List[(List[String], InPos)] = {
    InverseIndexBuilderImpl
      .buildIndexKeys(text)
      .sliding(hyper.textSplitLength, hyper.textSplitStep)
      .zipWithIndex
      .map { case ((tokens, chunkID)) => (tokens, chunkID * hyper.textSplitStep) }
      .toList
  }

  /**
    * Returns each unique token
    *
    * @param tokens The list of tokens
    * @return       A set of words, which occurs in the chunk
    */
  def groupTokens(tokens: (List[String], InPos)): (Set[String], InPos) = {
    val (tokensData, tokensInPos) = tokens
    (tokensData.toSet, tokensInPos)
  }

  /**
    * Returns the (documentId, List[Positions]) Tupels from the index for each token
    * In fact it returns for each token the documentIDs and positions where the token occurs
    *
    * Example:
    * ((das, 2), =>    List[ List[(1, List[10, 18, 35]), (2, List[4, 17])...],
    * (ist, 1),              List[(2, List[18], ...
    * (ein, 1),              ...
    * (plagiat,1))          ...]
    *
    * @param tokensBag Relevant tokens
    * @return          A List of Lists for each Token with its DocumentIds and Positions where they occurs
    */
  def getIndexValues(index: List[(String, List[(ID, List[ArtPos])])], tokensBag: (Set[String], InPos))
  : (List[List[(ID, List[ArtPos])]], InPos) = {

    val (tokenBag, inPos) = tokensBag
    (index.filter { case (key, _) => tokenBag.contains(key) }.map { case ((_, value)) => value }, inPos)
  }


  /**
    * Groups the Tokens Positions by DocumentId.
    * Returns the documentId with a list of its words positions
    *
    * Example:
    * List((1, 10), (2, 4), (3, 15), (4, 2))
    * List((1, 11), (2, 6), (3, 20))
    * List((1, 12), (2, 5))
    * List((1, 13), (3, 3))
    * =>  (2, List(4, 6, 5))
    * (4, List(2))
    * (1, List(10, 11, 12, 13))
    * (3, List(15, 20, 3))
    *
    *
    * @param indexValues Values for each token in the index
    * @return            Index values grouped by the documentIds
    */
  def groupByDocumentId(indexValues: (List[List[(ID, List[ArtPos])]], InPos)): (List[(ID, Iterable[ArtPos])], InPos) = {
    val (index, inPos) = indexValues
    val groupedByID: List[(PlagiarismFinder.ID, Iterable[PlagiarismFinder.ArtPos])] =
      index.flatten.groupBy[PlagiarismFinder.ID](_._1).mapValues(_.flatMap(_._2)).toList
    (groupedByID, inPos)
  }

  /**
    * Returns the DocumentIds which fulfill the minimum number of matching words
    *
    * @param indexValues Extracted values for each token from the index
    * @param hyper       Hyperparameter
    * @return            A list of fulfilling DocumentIds
    */
  def getRelevantDocuments(indexValues: (List[List[(ID, List[ArtPos])]], InPos), hyper: Hyper): Iterable[ID] = {
    val (index, _) = indexValues

    // The minimum number of matching tokens for further processing
    val minimumNumberMatchingWords: Int = (index.size * hyper.matchingWords).toInt

    // Create a list of IDs which come from different words
    val allIDs: List[ID] = index.flatMap(_.map { case (id, _) => id }.distinct)

    // Count the number of matching tokens by each documentId
    val numberMatchingTokensByDocumentId: Map[PlagiarismFinder.ID, Long] =
      allIDs.map(x => (x, 1)).groupBy(_._1).mapValues(_.size)

    // Return documentIds which fulfill the minimumnumberMatchingWords
    numberMatchingTokensByDocumentId.filter(_._2 >= minimumNumberMatchingWords).keys
  }

  /**
    * Filters on the minimum number of matching Words
    *
    * (2, List(4, 6, 5))
    * (4, List(2))
    * (1, List(10, 11, 12, 13))
    * (3, List(15, 20, 3))
    * => (2, List(4, 6, 5))
    *    (1, List(10, 11, 12, 13))
    *    (3, List(15, 20, 3))
    *
    *
    * @param groupedDocumentIds List of documentIds and their positions to filter
    * @param indexValues        List of Tokens with their (DocumentId,Position) Tupels
    * @param hyper              Hyperparameter
    * @return                   Filtered list of documentIds and their positions
    */
  def filterRelevantDocuments(
    groupedDocumentIds: (List[(ID, Iterable[ArtPos])], InPos),
    indexValues: (List[List[(ID, List[ArtPos])]], InPos),
    hyper: Hyper
  ): (List[(ID, Iterable[ArtPos])], InPos) = {

    val (docIDs, inPos) = groupedDocumentIds
    val relevantDocumentsList: List[ID] = getRelevantDocuments(indexValues, hyper).toList
    (docIDs.filter { case (id, _) => relevantDocumentsList.contains(id) }, inPos)
  }

  /**
    * Filters on the maximal Distance between tokens and returns (DocumentId, (Position,Distance)) tuples
    *
    * (2, List(4, 6, 5))
    * (1, List(10, 11, 12, 13))
    * (3, List(15, 20, 3))
    * =>
    * (2, List((4, 0), (5, 1), (6, 1)))
    * (1, List((10, 0), (11, 1), (12, 1), (13, 1)))
    *
    *
    * @param relevantDocuments Documents to be filtered
    * @param hyper             Hyperparameter
    * @return                  (DocumentId, (Position, Distance)) tuples
    */
  def filterMaximalDistance(relevantDocuments: (List[(ID, Iterable[ArtPos])], InPos), hyper: Hyper)
  : (List[(ID, List[(ArtPos, Delta)])], InPos) = {

    // some magic happens here
    val (docs, inPos) = relevantDocuments
    val sorted: List[(ID, List[ArtPos])] = docs.map { case ((id, posL)) => (id, posL.toList.sorted) }
    val positionAndPredecessorPosition: List[(ID, List[(ArtPos, ArtPos)])] =
      sorted.map { case ((id, posL)) => (id, (posL.head, posL.head) :: posL.tail.zip(posL)) }
    (positionAndPredecessorPosition
      .map { case ((id, posL)) =>
        val posLwithD = posL.map { case ((pre, post)) => (pre, pre - post) }
        (id, posLwithD.zip(posLwithD.tail))
      }
      .map { case ((id, deltas)) =>
        val ((deltasHPos, _), _) = deltas.head
        val ( _, (deltasLPos, _)) = deltas.last
        (id, deltas.filter {
          case ((fPos, deltaL), (lPos, deltaR)) =>
            if (deltasHPos == fPos)      deltaL <= hyper.maxDistanceAlpha && deltaR <= hyper.maxDistanceAlpha
            else if (deltasLPos == lPos) deltaR <= hyper.maxDistanceAlpha
            else                         deltaL <= hyper.maxDistanceAlpha || deltaR <= hyper.maxDistanceAlpha
        })
      }
      .filter { case ((_, deltas)) => deltas.nonEmpty }
      .map { case ((id, deltasLR)) => (id,  deltasLR.map(_._1) :+ deltasLR.last._2)}, inPos)
  }

  /**
    * Computes Distances between filtered positions for further finding of text regions
    * with a significant accumulation of words
    *
    * (2, List((5, 1), (6, 1)))
    * (1, List((11, 1), (12, 1), (13, 1), (52, 2), (53, 1)))
    * =>
    * (2, List((5, 0), (6, 1)))
    * (1, List((11, 1), (12, 1), (13, 1), (52, 39), (53, 1)))
    *
    *
    * @param docPositionsWithSignificance DocumentIds with their positions and distances
    * @return Tupels of (documentId, List(Position,Distance to Predecessor))
    */

  def computeDistancesBetweenRelevantPositions(docPositionsWithSignificance: (List[(ID, List[(ArtPos, Delta)])], InPos))
  : (List[(ID, List[(ArtPos, Delta)])], InPos) = {

    val (docs, inPos) = docPositionsWithSignificance

    // Filter out the outliners
    val filteredOutliners: List[(ID, List[(ArtPos, Delta)])] = docs.filter { case ((_, deltas)) => !(deltas.length < 2) }

    // Get documentId and only positions (not distances to predecessor)
    val positions: List[(ID, List[ArtPos])] =
      filteredOutliners.map { case ((id, deltas)) => (id, deltas.map { case (artPos, _) => artPos }) }

    // Create tupels of (predecessor position, position)
    val positionAndPredecessorPosition: List[(ID, List[(ArtPos, ArtPos)])] =
      positions.map { case ((id, posL)) => (id, (posL.head, posL.head) :: posL.zip(posL.tail)) }

    // DocumentId with tupels of (position, distance to predecessor)
    (positionAndPredecessorPosition.map {
      case ((id, posLT)) => (id, posLT.map { case ((pre, post)) => (pre, post - pre) })
    }, inPos)
  }

  /**
    * Groups suspicious Positions into Regions with high density.
    * These regions indicate a plagiarism
    *
    * @param distances Tupels of (documentId, List(Position, Distance to Predecessor))
    * @param hyper     Hyperparameter
    * @return List of DocumentID and Positions
    */
  def splitIntoRegions(distances: (List[(ID, List[(ArtPos, Delta)])], InPos), hyper: Hyper)
  : (List[(ID, List[(ArtPos, Delta)])], InPos) = {

    val (dists, inPos) = distances
    var groupID = 0
    (for {
      (text_id, text_pos) <- dists
      (_, chunk) <- text_pos.groupBy[Int] { case ((_, delta)) =>
        if (delta > hyper.maxDistanceBeta) groupID += 1
        groupID
      }.toSeq
      if chunk.size >= hyper.minGroupSize
    } yield (text_id, chunk), inPos)
  }

  /**
    * Groups suspicious Positions into Regions with high density.
    * These regions indicate a plagiarism
    *
    * @param positions List of (documentId, List(Position, Article positions))
    * @param hyper     Hyperparameter
    * @return          List of DocumentID and Positions
    */
  def splitIntoRegions(positions: List[(ID, List[(InPos, (ArtPos, ArtPos, Double), Delta)])], hyper: Hyper)
  : List[(ID, List[(InPos, (ArtPos, ArtPos, Double), Delta)])] = {

    var groupID = 0
    for {
      (text_id, text_pos) <- positions
      (_, chunk) <- text_pos.groupBy[(ID, ArtPos)] { case ((artPosS, _, delta)) =>
        if (delta > hyper.maxDistanceBeta) groupID += 1
        (groupID, artPosS)
      }.toSeq
      if chunk.size >= hyper.minGroupSize
    } yield (text_id, chunk)
  }

  /**
    * Returns regions with naive start- and endpoints
    *
    * @param regions DocumentID with List of Positions and Distances
    * @return List of Regions defined by DocumentID, start- and endpoints and score
    */
  def getPointerToRegions(regions: (List[(ID, List[(ArtPos, Delta)], Double)], InPos))
  : List[(InPos, ID, (ArtPos, ArtPos, Double))] = {

    val (regs, inPos) = regions
    regs.map { case ((id, r, score)) => (inPos, id, (r.head._1, r.last._1, score)) }
  }

  /**
    * Returns regions with naive start- and endpoints
    *
    * @param regions   DocumentID with List of Positions and Distances
    * @param hyper     Hyperparameter
    * @return          List of Regions defined by DocumentID, start- and endpoints and score
    */
  def getPointerToRegions(regions: List[(ID, List[(InPos, (ArtPos, ArtPos, Double), Delta)])], hyper: Hyper)
  : List[((InPos, ID, ArtPos), (InPos, ID, ArtPos), Double)] = {

    regions.map{
      case ((id, regs)) =>
        val ((inPosH, (artPosHS, _, score), _), (inPosL, ( _, artPosLE, _), _)) = (regs.head, regs.last)
        ((inPosH, id, artPosHS), (inPosL + hyper.textSplitStep, id, artPosLE), score) }
  }

  var i = 0
  /**
    * Initiates the Check for Plagiarism Algorithm
    * Calls several functions and returns a list of DocumentIDs and Positions where a Plagiarism is detected
    *
    * @param index     Inverse Index Part
    * @param tokens    List of Tokens to check
    * @param hyper     Hyperparameter
    * @return          Matches of potential plagiarisms
    */
  def checkForPlagiarism(index: List[(String, List[(ID, List[ArtPos])])], tokens: ((List[String], InPos), Int), hyper: Hyper)
  : List[(InPos, ID, (ArtPos, ArtPos, Double))] = {

    // Printing progress
    println(f"Processing [$i of $nChunks] ...")

    // Fetching unique tokens
    val tokensBag: (Set[String], InPos) = groupTokens(tokens._1)

    // Fetching sub index for this chunk
    val indexValues: (List[List[(ID, List[ArtPos])]], InPos) = getIndexValues(index, tokensBag)

    // Grouping by document id
    val groupedDocumentIds: (List[(ID, Iterable[ArtPos])], InPos) = groupByDocumentId(indexValues)

    // Filtering documents and keeping only these which are relevant to us
    val relevantDocuments: (List[(ID, Iterable[ArtPos])], InPos) =
      filterRelevantDocuments(groupedDocumentIds, indexValues, hyper)

    // Filtering outliners out of the documents
    val relevantDocumentsWithSignificance : (List[(ID, List[(ArtPos, Delta)])], InPos) =
      filterMaximalDistance(relevantDocuments, hyper)

    // Computing the distances between words
    val newDistances: (List[(ID, List[(ArtPos, Delta)])], InPos) =
      computeDistancesBetweenRelevantPositions(relevantDocumentsWithSignificance)

    // Splitting the list of positions into regions
    val splittedRegions: (List[(ID, List[(ArtPos, Delta)])], InPos) = splitIntoRegions(newDistances, hyper)

    // Calculating the score [experimental]
    val scoredRegions: (List[(ID, List[(ArtPos, Delta)], Double)], InPos) = calcScore(splittedRegions, hyper)

    // Returning the pointers (start & end)  to the calculated regions
    val result: List[(InPos, ID, (ArtPos, ArtPos, Double))] = getPointerToRegions(scoredRegions)

    // Updating the current processing chunk id
    i += 1
    result
  }

  /**
    * Calculating the score, which classify the significance of the match
    *
    * x = 1            - Exact match
    * x = 0.5          - 2 words distance in average
    * x = 0.2          - 5 words distance in average
    * x < 0 + epsilon  - big distance in average (likely no match at all)
    *
    * [experimental]
    *
    * @param regions   DocumentID with List of Positions and Distances
    * @param hyper     Hyperparameter
    * @return          Scored matches
    */
  def calcScore(regions: (List[(ID, List[(ArtPos, Delta)])], InPos), hyper: Hyper)
  : (List[(ID, List[(ArtPos, Delta)], Double)], InPos) = {

    val (regs, inPos) = regions
    (regs.map{ case ((id, reg)) => (id, reg, reg.size / reg.foldLeft(0.0)(_ + _._2)) }, inPos)
  }
}