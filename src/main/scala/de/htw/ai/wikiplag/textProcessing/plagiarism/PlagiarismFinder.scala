package de.htw.ai.wikiplag.textProcessing.plagiarism


import de.htw.ai.wikiplag.data.{InverseIndexBuilderImpl, MongoDbClient}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by _ on 11/2/16.
  */
class PlagiarismFinder extends Serializable {
  /**
    * Starts the PlagiarismFinder Object
    * Wraps the functions of the Object.
    *
    * @param sc spark context
    * @param inputText text to process
    * @param h_textSplitLength number of tokens per slice
    * @param h_textSplitStep  stepsize for slicing process (overlap)
    * @param h_matchingWordsPercentage the minimum percentage of matching words to fulfill
    * @param h_maximalDistance the maximal distance between words to be considered in further processing
    * @param h_maxNewDistance maximal distance between regions
    * @param h_minGroupSize minimum size of a relevant group
    */
  def apply(sc: SparkContext, inputText: String, h_textSplitLength: Int = 20, h_textSplitStep: Int = 15,
            h_matchingWordsPercentage: Double = 0.70, h_maximalDistance: Int = 3, h_maxNewDistance: Int = 7,
            h_minGroupSize: Int = 10): List[((Int, Long, Int), (Int, Long, Int), Double)] = {

    // todo: Password auslagern
    val client = MongoDbClient(sc, "hadoop03.f4.htw-berlin.de", 27020, "wikiplag", "wikiplag", "Ku7WhY34")
    MongoDbClient.open()
    val index = client.getInvIndexRDD(InverseIndexBuilderImpl.buildIndexKeySet(inputText))

    /*
    Here we call our function
     */
    val textParts = PlagiarismFinder.splitText(inputText, h_textSplitLength, h_textSplitStep)
    val tmp = textParts.flatMap(x => PlagiarismFinder.checkForPlagiarism(index, sc, x, h_matchingWordsPercentage, h_maximalDistance, h_maxNewDistance, h_minGroupSize, h_textSplitStep))

    val tmp2 = tmp.groupBy(_._2).mapValues(x => {
      val y = x.sortBy(_._3)
      (y.head._2, (y.head._1, y.head._3, 0)) :: y.zip(y.tail).map(z => (z._2._2, (z._2._1, z._2._3, z._2._3 - z._1._3)))
    }).toList.map(x => (x._1, x._2.map(y => (y._2._1, y._2._2, y._2._3))))

    val t3 = PlagiarismFinder.getPointerToRegions(PlagiarismFinder.splitIntoRegions(tmp2, 50, 0), h_textSplitStep)
    t3.map(x => (x._1, x._2, 0.0))
  }
}

object PlagiarismFinder extends Serializable {
  type InPos = Int
  type ID = Long
  type WikPos = Int
  type Delta = Int

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
  def getIndexValues(index: RDD[(String, List[(ID, List[WikPos])])], sc: SparkContext, tokensMap: (Map[String, Int], InPos)): (List[List[(ID, List[WikPos])]], InPos) = {
    val _tm = tokensMap._1.map(identity)
    (index.filter(x => _tm.contains(x._1)).map(_._2).collect().toList, tokensMap._2)
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
  def groupByDocumentId(indexValues: (List[List[(ID, List[WikPos])]], InPos)): (List[(ID, List[WikPos])], InPos) =
    (indexValues._1.flatten.groupBy(_._1).mapValues(_.flatMap(_._2)).toList, indexValues._2)

  /**
    * Returns the Number of Matching Tokens (Tokens which were extracted from the Index)
    *
    * @param indexValues the values for each token in the index
    * @return the number of tokens
    */
  def countMatchingTokens(indexValues: (List[List[(ID, List[WikPos])]], InPos)): Int =
    indexValues._1.length

  /**
    * Returns the DocumentIds which fulfill the minimum number of matching words
    *
    * @param indexValues               the extracted values for each token from the index
    * @param h_matchingWordsPercentage the minimum percentage of matching words to fulfill
    * @return a list of fulfilling DocumentIds
    */
  def getRelevantDocuments(indexValues: (List[List[(ID, List[WikPos])]], InPos), h_matchingWordsPercentage: Double): List[ID] = {
    //number of tokens extracted from the index
    val numberMatchingTokens = countMatchingTokens(indexValues)
    //the minimum number of matching tokens for further processing
    val minimumNumberMatchingWords = (numberMatchingTokens * h_matchingWordsPercentage).toInt
    //create a set per token value
    val valuesToSet = indexValues._1.flatMap(l => l.map(v => v._1).toSet)
    //count the number of matching tokens by each documentId
    val numberMatchingTokensByDocumentId = valuesToSet.groupBy(documentId => documentId).mapValues(_.size)
    //return documentIds which fulfill the minimumnumberMatchingWords
    val result = numberMatchingTokensByDocumentId.filter(_._2 >= minimumNumberMatchingWords).toList.map(x => x._1)
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
  def filterRelevantDocuments(groupedDocumentIds: (List[(ID, List[WikPos])], InPos), indexValues: (List[List[(ID, List[WikPos])]], InPos),
                              h_matchingWordsPercentage: Double): (List[(ID, List[WikPos])], InPos) = {
    val relevantDocumentsList = getRelevantDocuments(indexValues, h_matchingWordsPercentage)
    (groupedDocumentIds._1.filter { case (k, _) => relevantDocumentsList.contains(k) }, indexValues._2)
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
  def filterMaximalDistance(relevantDocuments: (List[(ID, List[WikPos])], InPos), h_maximalDistance: Int): (List[(ID, List[(WikPos, Delta)])], InPos) = {
    //slices the sorted list of positions into (predecessorposition, postion) slices
    val positionAndPredecessorPosition = relevantDocuments._1.map(x => (x._1, x._2.sorted.sliding(2).toList))
    //maps the tupels on (position, distance to predecessor) tupels
    try {
      (positionAndPredecessorPosition.map(x => (x._1, x._2.filter(_.size > 1).map(y => (y(1), y(1) - y.head)))).map(x => (x._1, x._2.filter(y => y._2 <= h_maximalDistance))).filter(x => x._2.nonEmpty), relevantDocuments._2)
    } catch {
      case _:Exception => println(positionAndPredecessorPosition); (List(), -1)
    }
    //filters on the h_maximalDistance and documentIds with no single fulfilling word are filtered out afterwards
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
    val filteredSingleTupels = relevantDocumentsWithSignificance._1.filterNot(_._2.length < 2)
    //get documentId and only positions (not distances to predecessor)
    val positions = filteredSingleTupels.map(x => (x._1, x._2.map(y => y._1)))
    //create tupels of (predecessor position, position)
    val positionAndPredecessorPosition = positions.map(x => (x._1, x._2.sorted.sliding(2).toList))
    //documentId with tupels of (position, distance to predecessor)

    (positionAndPredecessorPosition.map(x => (x._1, x._2.filter(_.size > 1).map(y => (y(1), y(1) - y(0))))), relevantDocumentsWithSignificance._2)
  }


  /**
    * Initiates the Check for Plagiarism Algorithm
    * Calls several functions and returns a list of DocumentIDs and Positions where a Plagiarism is detected
    *
    * @param tokens  list of Tokens to check
    * @param h_matchingWordsPercentage the minimum percentage of matching words to fulfill
    * @param h_maximalDistance the maximal distance between words to be considered in further processing
    * @param h_maxNewDistance maximal distance between regions
    * @param h_minGroupSize
    * @return minimum size of a relevant group
    */
  def checkForPlagiarism(index: RDD[(String, List[(ID, List[WikPos])])], sc: SparkContext, tokens: (List[String], InPos), h_matchingWordsPercentage: Double,
                         h_maximalDistance: Int, h_maxNewDistance: Int, h_minGroupSize: Int, h_textSplitStep: Int): List[(InPos, ID, WikPos)] = {
    println("start finding")
    val tokensMap = groupTokens(tokens)
    val indexValues = getIndexValues(index, sc, tokensMap)
    val groupedDocumentIds = groupByDocumentId(indexValues)
    val relevantDocuments = filterRelevantDocuments(groupedDocumentIds, indexValues, h_matchingWordsPercentage)
    val relevantDocumentsWithSignificance = filterMaximalDistance(relevantDocuments, h_maximalDistance)
    val newDistances = computeDistancesBetweenRelevantPositions(relevantDocumentsWithSignificance)
    val splittedRegions = splitIntoRegions(newDistances, h_maxNewDistance, h_minGroupSize)
    val result = getPointerToRegions(splittedRegions, h_textSplitStep)
    println("end finding")
    result
  }


  /**
    * Groupes suspicious Positions into Regions with high density.
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

  def splitIntoRegions(newDistances: List[(ID, List[(InPos, WikPos, Delta)])],
                       h_maxNewDistance: Int, h_minGroupSize: Int): List[(ID, List[(InPos, WikPos, Delta)])] = {
    var key = 0
    for {
      (text_id, text_pos) <- newDistances
      chunk <- text_pos.groupBy(dist => {
        if (dist._3 > h_maxNewDistance) key += 1; key
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
  def getPointerToRegions(regions: (List[(ID, List[(WikPos, Delta)])], InPos), h_textSplitStep: Int): List[(InPos, ID, WikPos)] =
    regions._1.map(r => (regions._2, r._1, r._2.head._1))

  def getPointerToRegions(regions: List[(ID, List[(InPos, WikPos, Delta)])], h_textSplitStep: Int): List[((InPos, ID, WikPos), (InPos, ID, WikPos))] =
    regions.map(r => ((r._2.head._1, r._1, r._2.head._2), (r._2.last._1 + h_textSplitStep, r._1, r._2.last._2)))
}