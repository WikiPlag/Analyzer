package de.htw.ai.wikiplag.textProcessing.plagiarism

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Flow, Sink, _}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.concurrent.{Await, ExecutionContext, Future}
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import akka.util.ByteString
import de.htw.ai.wikiplag.textProcessing.Tokenizer
import de.htw.ai.wikiplag.textProcessing.indexer.WikiplagIndex

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps

/**
  * Created by _ on 11/2/16.
  */
object PlagiarismFinder {
  type Plagiat = String
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

  class Chunker(val h_textSplitLength: Int, val h_textSplitStep: Int) extends GraphStage[FlowShape[Plagiat, Parts]] {

    val (in, out) = (Inlet[Plagiat]("Chunker.in"), Outlet[Parts]("Chunker.out"))
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

  def fetchFromIndex(h_matchingWordsPercentage: Double): Flow[Parts, PagesRaw, NotUsed] =
    Flow[Parts].map(r => r.foldLeft(List[PageRaw]())((a, x) => a ::: index.getOrElse(x, List()))
      .groupBy(_._1).toList.map(p => (p._1, p._2.foldLeft(List[Position]())((l, r) => l ::: r._2)))
      .map(p => (p._1, p._2.distinct))
      .filter(v => r.size * h_matchingWordsPercentage < v._2.size)).filter(_.nonEmpty)

  def sort(): Flow[PagesRaw, PagesRaw, NotUsed] = Flow[PagesRaw].map(_.map(p => (p._1, p._2.sorted)))

  def computeDistPre(): Flow[PagesRaw, PagesDist, NotUsed] =
    Flow[PagesRaw].map(_.map(p => (p._1, (p._2.head, 0) :: p._2.zip(p._2.tail).map(x => (x._2, x._2 - x._1)))))

  def filterPre(h_maxDistance: Int, h_minGroupSize: Int): Flow[PagesDist, PagesDist, NotUsed] =
    Flow[PagesDist].map(_.map(p => (p._1, p._2.filter(_._2 <= h_maxDistance))).filter(_._2.size >= h_minGroupSize))

  def computeDistPost(): Flow[PagesDist, PagesDist, NotUsed] =
    Flow[PagesDist].map(_.map(p => (p._1, (p._2.head._1, 0) :: p._2.zip(p._2.tail).map(x => (x._2._1, x._2._1 - x._1._1)))))

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

  def cleanRegions(): Flow[PagesDist, PagesDist, NotUsed] =
    Flow[PagesDist].map(_.map(p => (p._1, (p._2.head._1, 0) :: p._2.tail)))

  def filterPost(h_maxAverageDistance: Double, h_minGroupSize: Int): Flow[PagesDist, PagesDist, NotUsed] =
    Flow[PagesDist].filter(_.exists(_._2.sliding(h_minGroupSize, (h_minGroupSize * 0.7).toInt)
      .filter(_.size > h_minGroupSize * 0.6)
      .exists(l => l.foldLeft(0)(_ + _._2) / l.size < h_maxAverageDistance)))

  def processing(h_textSplitLength: Int = 20,
                 h_textSplitStep: Int = 15,
                 h_matchingWordsPercentage: Double = 0.7,
                 h_maxDistance: Int = 5,
                 h_minGroupSize: Int = 13): Flow[Plagiat, PagesDist, NotUsed] = {
    Flow[Plagiat].via(new Chunker(h_textSplitLength, h_textSplitStep))
      .via(balanceProcessing(fetchFromIndex(h_matchingWordsPercentage).via(sort()).via(computeDistPre())
                               .via(filterPre(h_maxDistance, h_minGroupSize)).via(computeDistPost()), 100))
      .via(splitIntoRegions(h_maxDistance, h_minGroupSize)).via(cleanRegions()).via(filterPost(1.2, h_minGroupSize))
  }

  def start(id: Int, plagiat: Plagiat): Future[Seq[(Int, PagesDist)]] = {
    println(id + ": " + plagiat.take(10) + "...")
    Source(Tokenizer.tokenize(plagiat))
      .via(processing()).collect { case x if x.nonEmpty => x }
        .limit(1000).map((id, _)).runWith(Sink.seq)
  }
}
