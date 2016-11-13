package de.htw.ai.wikiplag.textProcessing.plagiarism

import de.htw.ai.wikiplag.textProcessing.Tokenizer
import de.htw.ai.wikiplag.textProcessing.indexer.WikiplagIndex

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._

import scala.concurrent.Future
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps

/**
  * Object which helps to find plagiary
  *
  * Created by _ on 11/2/16.
  * Reworked by Kuro on 13/11/16
  */
object PlagiarismFinder {
  type Plagiary = String
  type Parts = List[String]
  type ID = BigInt
  type Position = Int
  type Distance = Int
  type PageRaw = (ID, List[Position])
  type PagesRaw = List[PageRaw]
  type PageDist = (ID, List[(Position, Distance)])
  type PagesDist = List[PageDist]

  implicit val system = ActorSystem("QuickStart")
  implicit val materializer = ActorMaterializer()
  val index = WikiplagIndex("mehrere_pages_klein.xml")

  /**
    * Input-Text -> Chunks for processing
    *
    * Buffers text and emits chunks of size [h_textSplitLength].
    * Each chunk have an offset of [h_textSplitLength].
    *
    * Todo: Maybe upperbound the Buffer for backpressure.
    *
    * @param h_textSplitLength Size of emitted chunks
    * @param h_textSplitStep Offset of each chunk
    */
  class Chunker(val h_textSplitLength: Int, val h_textSplitStep: Int) extends GraphStage[FlowShape[Plagiary, Parts]] {

    val (in, out) = (Inlet[Plagiary]("Chunker.in"), Outlet[Parts]("Chunker.out"))
    override val shape = FlowShape.of(in, out)

    override def createLogic(attr: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      private var buffer = ListBuffer[String]()
      setHandler(out, new OutHandler {
        @scala.throws[Exception](classOf[Exception])
        override def onPull(): Unit = {
          if (isClosed(in)) emitChunk()
          else pull(in)
        }
      })

      setHandler(in, new InHandler {
        @scala.throws[Exception](classOf[Exception])
        override def onPush(): Unit = {
          buffer += grab(in)
          emitChunk()
        }

        override def onUpstreamFinish(): Unit = {
          if (buffer.isEmpty) completeStage()
          else if (isAvailable(out)) emitChunk()
        }
      })

      private def emitChunk(): Unit = {
        if (buffer.isEmpty) {
          if (isClosed(in)) completeStage()
          else pull(in)
        } else if (buffer.size > h_textSplitLength) {
          val chunk = buffer.take(h_textSplitLength).toList
          buffer = buffer.drop(h_textSplitStep)
          push(out, chunk)
        } else if (!isClosed(in)) {
          pull(in)
        } else {
          val chunk = buffer.take(h_textSplitLength).toList
          buffer = buffer.drop(h_textSplitStep)
          push(out, chunk)
        }
      }
    }
  }

  /**
    * Assign work to [workerCount] workers.
    *
    * @param worker Work to do
    * @param workerCount Number of workers
    * @tparam In InputType of the work
    * @tparam Out OutputType of the work
    * @return Balanced work as Flow
    */
  def balanceProcessing[In, Out](worker: Flow[In, Out, Any], workerCount: Int): Flow[In, Out, NotUsed] = {
    import GraphDSL.Implicits._

    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      val balancer = builder.add(Balance[In](workerCount, waitForAllDownstreams = true))
      val merger = builder.add(Merge[Out](workerCount))

      for (_ <- 1 to workerCount)
        balancer ~> worker.async ~> merger

      FlowShape(balancer.in, merger.out)
    })
  }

  /**
    * Takes chunks of InputText and get for each Word the PageIDs and the Positions, where they are located from
    * the Index. At the same time it filters the Pages with a minimum of occurrences of words.
    *
    * @param h_matchingWordsRatio Proportion of words, which must occur in pages
    * @return Flow, which processes chunks to a list of PageIDs and positions
    */
  def fetchFromIndex(h_matchingWordsRatio: Double): Flow[Parts, PagesRaw, NotUsed] =
    Flow[Parts].map(r => r.foldLeft(List[PageRaw]())((a, x) => a ::: index.getOrElse(x, List()))
      .groupBy(_._1).toList.map(p => (p._1, p._2.foldLeft(List[Position]())((l, r) => l ::: r._2)))
      .map(p => (p._1, p._2.distinct))
      .filter(v => r.size * h_matchingWordsRatio < v._2.size)).filter(_.nonEmpty)

  /**
    * Sort the input
    *
    * @return Flow, which sorts the incoming positions of the pages
    */
  def sort(): Flow[PagesRaw, PagesRaw, NotUsed] = Flow[PagesRaw].map(_.map(p => (p._1, p._2.sorted)))

  /**
    * Computes the distances between the words. (Pre)
    *
    * @return Flow, which computes the distances between the words
    */
  def computeDistPre(): Flow[PagesRaw, PagesDist, NotUsed] =
    Flow[PagesRaw].map(_.map(p => (p._1, (p._2.head, 0) :: p._2.zip(p._2.tail).map(x => (x._2, x._2 - x._1)))))

  /**
    * Filters the incoming pages. Words with a distance greater than [h_maxDistance] the the predecessor will be
    * filtered out. Than the pages will be checked for their length. Only pages with min. [h_minGroupSize] words
    * will be passed through.
    *
    * @param h_maxDistance The max. distance to the word-predecessor
    * @param h_minGroupSize The min. size of the words in the page
    * @return Flow, which filters the incoming pages
    */
  def filterPre(h_maxDistance: Int, h_minGroupSize: Int): Flow[PagesDist, PagesDist, NotUsed] =
    Flow[PagesDist].map(_.map(p => (p._1, p._2.filter(_._2 <= h_maxDistance))).filter(_._2.size >= h_minGroupSize))

  /**
    * Computes the distances between the words. (Post)
    *
    * @return Flow, which computes the distances between the words
    */
  def computeDistPost(): Flow[PagesDist, PagesDist, NotUsed] =
    Flow[PagesDist].map(_.map(p => (p._1, (p._2.head._1, 0) :: p._2.zip(p._2.tail).map(x => (x._2._1, x._2._1 - x._1._1)))))

  /**
    * Splits the incoming page into regions. Regions with size less than [h_minGroupSize] will be filtered out.
    *
    * @param h_maxDistance Max distance, in which a group is consistent
    * @param h_minGroupSize Min size of group, which must be met
    * @return Flow, which groups the incoming pages into groups
    */
  def splitIntoRegions(h_maxDistance: Int, h_minGroupSize: Int): Flow[PagesDist, PagesDist, NotUsed] = {
    Flow[PagesDist].map({
      var key = 0
      for {
        (text_id, text_pos) <- _
        chunk <- text_pos.groupBy(dist => { if (dist._2 >= h_maxDistance) key += 1; key }).toSeq
        if chunk._2.size >= h_minGroupSize
      } yield (text_id, chunk._2)
    })
  }

  /**
    * Cleans the first distance of each group to 0.
    *
    * @return Flow, which sets the distance of the first word in the group to 0
    */
  def cleanRegions(): Flow[PagesDist, PagesDist, NotUsed] =
    Flow[PagesDist].map(_.map(p => (p._1, (p._2.head._1, 0) :: p._2.tail)))

  /**
    * Filters out the incoming groups, which have a distance of more than [h_maxAverageDistance] in average
    *
    * @param h_maxAverageDistance Max distance in average
    * @param h_minGroupSize Min size of the group
    * @return Flow, which filters incoming groups for the average-distance
    */
  def filterPost(h_maxAverageDistance: Double, h_minGroupSize: Int): Flow[PagesDist, PagesDist, NotUsed] =
    Flow[PagesDist].filter(_.exists(_._2.sliding(h_minGroupSize, (h_minGroupSize * 0.7).toInt)
      .filter(_.size > h_minGroupSize * 0.6)
      .exists(l => l.foldLeft(0)(_ + _._2) / l.size < h_maxAverageDistance)))

  /**
    * Process an incoming text to possible plagiarism-origins
    *
    * @param h_textSplitLength Size of chunks
    * @param h_textSplitStep Offset of each chunk
    * @param h_matchingWordsPercentage Proportion of words, which must occur in pages
    * @param h_maxDistance The max. distance to word-predecessors
    * @param h_minGroupSize Min size of groups
    * @return Flow, which analyze possible plagiarisms
    */
  def processing(h_textSplitLength: Int = 20,
                 h_textSplitStep: Int = 15,
                 h_matchingWordsPercentage: Double = 0.7,
                 h_maxDistance: Int = 5,
                 h_minGroupSize: Int = 13): Flow[Plagiary, PagesDist, NotUsed] = {
    Flow[Plagiary].via(new Chunker(h_textSplitLength, h_textSplitStep))
      .via(balanceProcessing(fetchFromIndex(h_matchingWordsPercentage).via(sort()).via(computeDistPre())
                               .via(filterPre(h_maxDistance, h_minGroupSize)).via(computeDistPost()), 100))
      .via(splitIntoRegions(h_maxDistance, h_minGroupSize)).via(cleanRegions()).via(filterPost(1.2, h_minGroupSize))
  }

  /**
    * Starts the analysis of possible plagiarisms
    *
    * @param id ID of the input-text
    * @param plagiat Text, which should be analyzed
    * @return Future, which should hold possible origins of plagiarisms
    */
  def start(id: Int, plagiat: Plagiary): Future[Seq[(Int, PagesDist)]] = {
    println(id + ": " + plagiat.take(10) + "...")
    Source(Tokenizer.tokenize(plagiat))
      .via(processing()).collect { case x if x.nonEmpty => x }
        .limit(1000).map((id, _)).runWith(Sink.seq)
  }
}
