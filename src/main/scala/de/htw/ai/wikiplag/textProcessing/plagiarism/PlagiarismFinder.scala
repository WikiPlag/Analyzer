package de.htw.ai.wikiplag.textProcessing.plagiarism


import de.htw.ai.wikiplag.data.{InverseIndexBuilderImpl, MongoDbClient}
import org.apache.spark.SparkContext

class Hyper(val h_textSplitLength: Int = 20, val h_textSplitStep: Int = 15,
            val h_matchingWordsPercentage: Double = 0.70, val h_maximalDistance: Int = 3, val h_maxNewDistance: Int = 7,
            val h_minGroupSize: Int = 10) extends Serializable {}

class Start(val positionInputText: PlagiarismFinder.InPos,
            val documentID: PlagiarismFinder.ID,
            val positionWikiText: PlagiarismFinder.WikPos) extends Serializable {
  override def toString: String = s"[InText: $positionInputText, DocID: $documentID, WikiText: $positionWikiText]"
}
class End(val positionInputText: PlagiarismFinder.InPos,
          val documentID: PlagiarismFinder.ID,
          val positionWikiText: PlagiarismFinder.WikPos) extends Serializable {
  override def toString: String = s"[InText: $positionInputText, DocID: $documentID, WikiText: $positionWikiText]"
}
/** One match with start, end and score  */
class Match(val start: Start, val end: End, val score: Double) {
  override def toString: String = s"[Start:\t $start \n End:\t $end \n\t Score: [$score]]"
}

/**
  * Created by _ on 11/2/16.
  */
object PlagiarismFinder extends Serializable {
  type InPos = Int
  type ID = Long
  type WikPos = Int
  type Delta = Int

  /**
    * Starts the PlagiarismFinder Object
    * Wraps the functions of the Object.
    *
    * @param sc spark context
    * @param inputText text to process
    * @param h_textSplitLength number of tokens per slice
    * @param h_textSplitStep  step size for slicing process (overlap)
    * @param h_matchingWordsPercentage the minimum percentage of matching words to fulfill
    * @param h_maximalDistance the maximal distance between words to be considered in further processing
    * @param h_maxNewDistance maximal distance between regions
    * @param h_minGroupSize minimum size of a relevant group
    * @return List of Matches
    */
  def apply(sc: SparkContext, inputText: String, h_textSplitLength: Int = 20, h_textSplitStep: Int = 15,
            h_matchingWordsPercentage: Double = 0.70, h_maximalDistance: Int = 3, h_maxNewDistance: Int = 7,
            h_minGroupSize: Int = 10): Map[ID, List[Match]] = {
    val client = MongoDbClient(sc, "hadoop03.f4.htw-berlin.de", 27020, "wikiplag", "wikiplag", "Ku7WhY34")
    MongoDbClient.open()

    val hyper = new Hyper(h_textSplitLength, h_textSplitStep, h_matchingWordsPercentage, h_maximalDistance,
                          h_maxNewDistance, h_minGroupSize)

    println("Start splitting Text ...")
    val textParts = PlagiarismFinder.splitText(inputText, h_textSplitLength, h_textSplitStep).zipWithIndex
    val indices = textParts.map(x => client.getInvIndexRDD(x._1._1.toSet))

    println("Done splitting Text ...")
    val processed = textParts.flatMap(x => {
      PlagiarismFinder.checkForPlagiarism(indices(x._2).collect.toList, x, hyper)
    })
    val cleaned: List[(ID, List[(InPos, (WikPos, WikPos, Double), Delta)])] = processed.groupBy(_._2).mapValues(x => {
      val y = x.sortBy(_._3._1)
      (y.head._2, (y.head._1, y.head._3, 0)) :: y.zip(y.tail).map(z => (z._2._2, (z._2._1, z._2._3, z._2._3._1 - z._1._3._2)))
    }).toList.map(x => (x._1, x._2.map(y => (y._2._1, y._2._2, y._2._3))))

    val regions = PlagiarismFinder.splitIntoRegions(cleaned, 50, 0)
    val pointer = PlagiarismFinder.getPointerToRegions(regions, h_textSplitStep)
    pointer
      .map(x => new Match(new Start(x._1._1, x._1._2, x._1._3), new End(x._2._1, x._2._2, x._2._3), x._3))
      .groupBy(_.start.documentID)
      .mapValues(_.sortBy(_.start.positionInputText))
  }

  /**
    * Returns the tokens for a text.
    * The text gets tokenized.
    * Then its sliced into equal parts which are overlapping.
    * The task is parallelized by spark.
    *
    * @param text              text to process
    * @param h_textSplitLength number of tokens per slice
    * @param h_textSplitStep   stepsize for slicing process (overlap)
    * @return Lists with equal number of tokens per slice wrapped inside a RDD
    */
  def splitText(text: String, h_textSplitLength: Int, h_textSplitStep: Int): List[(List[String], InPos)] = {
    InverseIndexBuilderImpl.buildIndexKeys(text).map(_.toLowerCase).sliding(h_textSplitLength, h_textSplitStep).zipWithIndex.map(x => (x._1, x._2 * h_textSplitStep)).toList
  }

  /**
    * Returns each unique token and its number of occurrences
    *
    * Example:
    * ("das","ist","ein","plagiat") => ((das,1),(ist,1),(ein,1),(plagiat,1))
    *
    * @param tokens the list of tokens
    * @return a map with (token, occurrence) entries
    */
  def groupTokens(tokens: (List[String], InPos)): (Map[String, Int], InPos) = {
    (tokens._1.groupBy(identity).mapValues(_.size), tokens._2)
  }

  /**
    * Returns the (documentId,List[Positions]) Tupels from the index for each token
    * In fact it returns for each token the documentIDs and positions where the token occurs
    *
    * Example:
    * ((das,1), =>    List[ List[(1,List[10,18,35]),(2,List[4,17])...],
    * (ist,1),              List[(2,List[18], ...
    * (ein,1),              ...
    * (plagiat,1))          ...]
    * @param tokensMap the relevant tokens
    * @return A List of Lists for each Token with its DocumentIds and Positions where it occurs
    */
  def getIndexValues(index: List[(String, List[(ID, List[WikPos])])], tokensMap: (Map[String, Int], InPos)): (List[List[(ID, List[WikPos])]], InPos) = {
    val _tm = tokensMap._1.map(identity)
    (index.filter(x => _tm.contains(x._1)).map(_._2), tokensMap._2)
  }

  /**
    * Groups the Tokens Positions by DocumentId.
    * Returns the documentId with a list of its words positions
    *
    * Example:
    * List((1,10), (2,4), (3,15), (4,2))
    * List((1,11), (2,6), (3,20))
    * List((1,12), (2,5))
    * List((1,13), (3,3))                 =>  (2,List(4, 6, 5))
    * (4,List(2))
    * (1,List(10, 11, 12, 13))
    * (3,List(15, 20, 3))
    *
    * @param indexValues the values for each token in the index
    * @return the index Values grouped by the documentIds
    */
  def groupByDocumentId(indexValues: (List[List[(ID, List[WikPos])]], InPos)): (List[(ID, Iterable[WikPos])], InPos) = {
    val groupedByDID: Map[PlagiarismFinder.ID, Iterable[PlagiarismFinder.WikPos]] = indexValues._1.flatten.groupBy(_._1).mapValues(_.flatMap(_._2))
    (groupedByDID.toList, indexValues._2)
  }

  /**
    * Returns the Number of Matching Tokens (Tokens which were extracted from the Index)
    *
    * @param indexValues the values for each token in the index
    * @return the number of tokens
    */
  def countMatchingTokens(indexValues: (List[List[(ID, List[WikPos])]], InPos)): Int =
    indexValues._1.size

  /**
    * Returns the DocumentIds which fulfill the minimum number of matching words
    *
    * @param indexValues               the extracted values for each token from the index
    * @param h_matchingWordsPercentage the minimum percentage of matching words to fulfill
    * @return a list of fulfilling DocumentIds
    *
    */
  def getRelevantDocuments(indexValues: (List[List[(ID, List[WikPos])]], InPos), h_matchingWordsPercentage: Double): Iterable[ID] = {
    //the minimum number of matching tokens for further processing
    val minimumNumberMatchingWords: Int = (countMatchingTokens(indexValues) * h_matchingWordsPercentage).toInt
    //create a set per token value
    val allIDs: List[ID] = indexValues._1.flatMap(_.map(_._1).distinct)
    //count the number of matching tokens by each documentId
    val numberMatchingTokensByDocumentId: Map[PlagiarismFinder.ID, Long] = allIDs.map(x => (x, 1)).groupBy(_._1).mapValues(_.size)
    //return documentIds which fulfill the minimumnumberMatchingWords
    val result = numberMatchingTokensByDocumentId.filter(_._2 >= minimumNumberMatchingWords).keys
    result
  }

  /**
    * Filters on the minimum number of matching Words
    *
    * (2,List(4, 6, 5))
    * (4,List(2))
    * (1,List(10, 11, 12, 13))
    * (3,List(15, 20, 3))            => (2,List(4, 6, 5))
    * (1,List(10, 11, 12, 13))
    * (3,List(15, 20, 3))
    *
    * @param groupedDocumentIds        the List of documentIds and their positions to filter
    * @param indexValues               the List of Tokens with their (DocumentId,Position) Tupels
    * @param h_matchingWordsPercentage the minimum percentage of matching Words
    * @return the filtered list of documentIds and their positions
    */
  def filterRelevantDocuments(groupedDocumentIds: (List[(ID, Iterable[WikPos])], InPos), indexValues: (List[List[(ID, List[WikPos])]], InPos),
                              h_matchingWordsPercentage: Double): (List[(ID, Iterable[WikPos])], InPos) = {
    val relevantDocumentsList: List[ID] = getRelevantDocuments(indexValues, h_matchingWordsPercentage).toList
    (groupedDocumentIds._1.filter((kv: (ID, Iterable[WikPos])) => relevantDocumentsList.contains(kv._1)), indexValues._2)
  }

  /**
    * Filters on the maximal Distance between tokens and returns (DocumentId, (Position,Distance)) tuples
    *
    * (2,List(4, 6, 5))
    * (1,List(10, 11, 12, 13))
    * (3,List(15, 20, 3))         =>    (2,List((5,1), (6,1)))
    * (1,List((11,1), (12,1), (13,1)))
    *
    * @param relevantDocuments the documents to be filtered
    * @param h_maximalDistance the maximal distance between words to be considered in further processing
    * @return the (DocumentId, (Position,Distance)) tuples
    */
  def filterMaximalDistance(relevantDocuments: (List[(ID, Iterable[WikPos])], InPos), h_maximalDistance: Int): (List[(ID, List[(WikPos, Delta)])], InPos) = {
    val sortedRD: List[(ID, List[WikPos])] = relevantDocuments._1.map(x => (x._1, x._2.toList.sorted))
    val positionAndPredecessorPosition: List[(ID, List[(WikPos, WikPos)])] = sortedRD.map(x => (x._1, (x._2.head, x._2.head) :: x._2.zip(x._2.tail)))
    (positionAndPredecessorPosition
      .map(x => (x._1, x._2.map(y => (y._1, y._2 - y._1))))
      .map(x => (x._1, x._2.filter(y => y._2 <= h_maximalDistance)))
      .filter(x => x._2.nonEmpty), relevantDocuments._2)
  }

  /**
    * Computes Distances between filtered positions for further finding of text regions
    * with a significant accumulation of words
    *
    * (2,List((5,1), (6,1)))
    * (1,List((11,1), (12,1), (13,1), (52,2), (53,1)))  => (2,List((6,1)))
    * (1,List((12,1), (13,1), (52,39), (53,1)))
    *
    * @param relevantDocumentsWithSignificance documentIds with their positions and distances
    * @return tupels of (documentId, List(Position,Distance to Predecessor))
    */

  def computeDistancesBetweenRelevantPositions(relevantDocumentsWithSignificance: (List[(ID, List[(WikPos, Delta)])], InPos)): (List[(ID, List[(WikPos, Delta)])], InPos) = {
    val filteredSingleTupels: List[(ID, List[(WikPos, Delta)])] = relevantDocumentsWithSignificance._1.filter(x => !(x._2.length < 2))
    //get documentId and only positions (not distances to predecessor)
    val positions: List[(ID, List[WikPos])] = filteredSingleTupels.map(x => (x._1, x._2.map(y => y._1)))
    //create tupels of (predecessor position, position)
    val positionAndPredecessorPosition: List[(ID, List[(WikPos, WikPos)])] = positions.map(x => (x._1, (x._2.head, x._2.head) :: x._2.zip(x._2.tail)))
    //documentId with tupels of (position, distance to predecessor)

    (positionAndPredecessorPosition.map(x => (x._1, x._2.map(y => (y._1, y._2 - y._1)))), relevantDocumentsWithSignificance._2)
  }

  /**
    * Groups suspicious Positions into Regions with high density.
    * These regions indicate a plagiarism
    *
    * @param newDistances tupels of (documentId, List(Position,Distance to Predecessor))
    * @param h_maxNewDistance maximal distance between regions
    * @param h_minGroupSize   minimum size of a relevant group
    * @return List of DocumentID and Positions
    */
  def splitIntoRegions(newDistances: (List[(ID, List[(WikPos, Delta)])], InPos),
                       h_maxNewDistance: Int, h_minGroupSize: Int): (List[(ID, List[(WikPos, Delta)])], InPos) = {
    var key = 0
    (for {
      (text_id, text_pos) <- newDistances._1
      chunk <- text_pos.groupBy(dist => {
        if (dist._2 > h_maxNewDistance) key += 1; key
      }).toSeq
      if chunk._2.size >= h_minGroupSize
    } yield (text_id, chunk._2), newDistances._2)
  }

  def splitIntoRegions(newDistances: List[(ID, List[(InPos, (WikPos, WikPos, Double), Delta)])],
                       h_maxNewDistance: Int, h_minGroupSize: Int): List[(ID, List[(InPos, (WikPos, WikPos, Double), Delta)])] = {
    var key = 0
    for {
      (text_id, text_pos) <- newDistances
      chunk <- text_pos.groupBy(dist => {
        if (dist._3 > h_maxNewDistance) key += 1; (key, dist._1)
      }).toSeq
      if chunk._2.size >= h_minGroupSize
    } yield (text_id, chunk._2)
  }

  /**
    * Returns a single position from a region which we interpret as a naive pointer to the region
    *
    * @param regions documentID with List of Positions and Distances
    * @return List of Regions defined by DocumentID and a single Position
    */
  def getPointerToRegions(regions: (List[(ID, List[(WikPos, Delta)], Double)], InPos), h_textSplitStep: Int): List[(InPos, ID, (WikPos, WikPos, Double))] =
    regions._1.map(r => (regions._2, r._1, (r._2.head._1, r._2.last._1, r._3)))

  def getPointerToRegions(regions: List[(ID, List[(InPos, (WikPos, WikPos, Double), Delta)])], h_textSplitStep: Int): List[((InPos, ID, WikPos), (InPos, ID, WikPos), Double)] =
    regions.map(r => ((r._2.head._1, r._1, r._2.head._2._1), (r._2.last._1 + h_textSplitStep, r._1, r._2.last._2._2), r._2.head._2._3))

  var i = 0
  /**
    * Initiates the Check for Plagiarism Algorithm
    * Calls several functions and returns a list of DocumentIDs and Positions where a Plagiarism is detected
    *
    * @param tokens  list of Tokens to check
    * @return minimum size of a relevant group
    */
  def checkForPlagiarism(index: List[(String, List[(ID, List[WikPos])])], tokens: ((List[String], InPos), Int), hyper: Hyper): List[(InPos, ID, (WikPos, WikPos, Double))] = {
    println("start finding")
    val tokensMap = groupTokens(tokens._1)
    val indexValues: (List[List[(ID, List[WikPos])]], InPos) = getIndexValues(index, tokensMap)
    val groupedDocumentIds: (List[(ID, Iterable[WikPos])], InPos) = groupByDocumentId(indexValues)
    val relevantDocuments: (List[(ID, Iterable[WikPos])], InPos) = filterRelevantDocuments(groupedDocumentIds, indexValues, hyper.h_matchingWordsPercentage)
    val relevantDocumentsWithSignificance : (List[(ID, List[(WikPos, Delta)])], InPos) = filterMaximalDistance(relevantDocuments, hyper.h_maximalDistance)
    val newDistances: (List[(ID, List[(WikPos, Delta)])], InPos) = computeDistancesBetweenRelevantPositions(relevantDocumentsWithSignificance)
    val splittedRegions: (List[(ID, List[(WikPos, Delta)])], InPos) = splitIntoRegions(newDistances, hyper.h_maxNewDistance, hyper.h_minGroupSize)
    val scoredRegions: (List[(ID, List[(WikPos, Delta)], Double)], InPos) = calcScore(splittedRegions, hyper)
    val result: List[(InPos, ID, (WikPos, WikPos, Double))] = getPointerToRegions(scoredRegions, hyper.h_textSplitStep)
    println("end finding")
    i += 1
    result
  }

  def calcScore(regions: (List[(ID, List[(WikPos, Delta)])], InPos), hyper: Hyper): (List[(ID, List[(WikPos, Delta)], Double)], InPos) = {
    (regions._1.map(doc => (doc._1, doc._2, (doc._2.foldLeft(0.0)(_ + _._2) / doc._2.size) / hyper.h_maxNewDistance)), regions._2)
  }
}