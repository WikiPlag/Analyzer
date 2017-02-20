package de.htw.ai.wikiplag.textProcessing.plagiarism

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


/**
  * Created by kuro on 06.06.2016.
  */

@RunWith(classOf[JUnitRunner])
class PlagiarismFinderTest extends FunSuite {

  test("splitText empty") {
    val text = ""
    val hyper = new Hyper(textSplitLength = 5, textSplitStep = 2)
    val expected = List()
    val actual = PlagiarismFinder.splitText(text, hyper)
    assert(expected === actual)
  }

  test("splitText (5, 2) does not fit") {
    val text = "Text enthalten wenige Stopwörtern damit nix schwer per hand auszufiltern ..."
    val hyper = new Hyper(textSplitLength = 5, textSplitStep = 2)
    val expected = List(
      (List("text", "enthalten", "wenige", "stopwörtern", "damit"), 0),
      (List("wenige", "stopwörtern", "damit", "nix", "schwer"), 2),
      (List("damit", "nix", "schwer", "per", "hand"), 4),
      (List("schwer", "per", "hand", "auszufiltern"), 6))
    val actual = PlagiarismFinder.splitText(text, hyper)
    assert(expected === actual)
  }

  test("splitText (4, 3) fit") {
    val text = "Text enthalten wenige Stopwörtern damit nix schwer per hand auszufiltern ..."
    val hyper = new Hyper(textSplitLength = 4, textSplitStep = 3)
    val expected = List(
      (List("text", "enthalten", "wenige", "stopwörtern"), 0),
      (List("stopwörtern", "damit", "nix", "schwer"), 3),
      (List("schwer", "per", "hand", "auszufiltern"), 6))
    val actual = PlagiarismFinder.splitText(text, hyper)
    assert(expected === actual)
  }

  test("splitText (3, 3) no overlapping") {
    val text = "Text enthalten wenige Stopwörtern damit nix schwer per hand auszufiltern ..."
    val hyper = new Hyper(textSplitLength = 3, textSplitStep = 3)
    val expected = List(
      (List("text", "enthalten", "wenige"), 0),
      (List("stopwörtern", "damit", "nix"), 3),
      (List("schwer", "per", "hand"), 6),
      (List("auszufiltern"), 9))
    val actual = PlagiarismFinder.splitText(text, hyper)
    assert(expected === actual)
  }

  test("groupTokens empty") {
    val expected = (Set(), 0)
    val actual = PlagiarismFinder.groupTokens((List(), 0))
    assert(expected === actual)
  }

  test("groupTokens single") {
    val input = (List("ich", "bin", "ein", "ganz", "gewöhnlicher", "text"), 0)
    val expected = (Set("ich", "bin", "ein", "ganz", "gewöhnlicher", "text"), 0)
    val actual = PlagiarismFinder.groupTokens(input)
    assert(expected === actual)
  }

  test("groupTokens multi") {
    val input = (List("ich", "bin", "ein", "ganz", "gewöhnlicher", "text", "nicht",
      "mehr", "und", "nicht", "weniger", "nicht", "doch", "text"), 0)
    val expected = (Set("ich", "bin", "ein", "ganz", "gewöhnlicher", "text",
      "nicht", "mehr", "und", "weniger", "doch"), 0)
    val actual = PlagiarismFinder.groupTokens(input)
    assert(expected === actual)
  }

  test("getIndexValues empty index") {
    val index = List()
    val tokensBag = (Set("ich", "bin", "ein", "bag"), 0)
    val expected = (List(), 0)
    val actual = PlagiarismFinder.getIndexValues(index, tokensBag)
    assert(expected === actual)
  }

  test("getIndexValues empty bag") {
    val index: List[(String, List[(PlagiarismFinder.ID, List[PlagiarismFinder.ArtPos])])] = List(
      ("ich",   List((1, List(1, 4, 5)), (2, List(2, 5)))),
      ("bin",   List((1, List(2, 6)), (3, List(1, 4)))),
      ("ein",   List((2, List(1, 4, 5)), (2, List(1, 6)), (3, List(2, 5)))),
      ("index", List((1, List(3)), (4, List(66))))
    )
    val tokensBag = (Set[String](), 0)
    val expected = (List(), 0)
    val actual = PlagiarismFinder.getIndexValues(index, tokensBag)
    assert(expected === actual)
  }

  test("getIndexValues") {
    val index: List[(String, List[(PlagiarismFinder.ID, List[PlagiarismFinder.ArtPos])])] = List(
      ("ich",   List((1, List(1, 4, 5)), (2, List(2, 5)))),
      ("bin",   List((1, List(2, 6)), (3, List(1, 4)))),
      ("ein",   List((2, List(1, 4, 5)), (2, List(1, 6)), (3, List(2, 5)))),
      ("index", List((1, List(3)), (4, List(66))))
    )
    val tokensBag = (Set[String]("ich", "index"), 0)
    val expected = (List(List((1, List(1, 4, 5)), (2, List(2, 5))), List((1, List(3)), (4, List(66)))), 0)
    val actual = PlagiarismFinder.getIndexValues(index, tokensBag)
    assert(expected === actual)
  }

  test("groupByDocumentId empty") {
    val indexValues = (List(), 0)
    val expected = (List(), 0)
    val actual = PlagiarismFinder.groupByDocumentId(indexValues)
    assert(expected === actual)
  }

  test("groupByDocumentId") {
    val indexValues: (List[List[(PlagiarismFinder.ID, List[PlagiarismFinder.ArtPos])]], PlagiarismFinder.InPos) =
      (List(List((1, List(10, 22, 23)), (2, List(4)), (3, List(15)), (4, List(2))), List((1, List(11)),
        (2, List(6, 23)), (3, List(20))), List((1, List(12)), (2, List(5, 9, 10))), List((1, List(13)), (3, List(3)))), 0)
    val expected = (List((2, List(4, 5, 6, 9, 10, 23)), (4, List(2)),
      (1, List(10, 11, 12, 13, 22, 23)), (3, List(3, 15, 20))), 0)
    val actual = PlagiarismFinder.groupByDocumentId(indexValues)

    // Order is not important, but for comparison I sorted it
    assert(expected === (actual._1.map(x => (x._1, x._2.toList.sorted)), actual._2))
  }

  test("getRelevantDocuments restricted") {
    val indexValues: (List[List[(PlagiarismFinder.ID, List[PlagiarismFinder.ArtPos])]], PlagiarismFinder.InPos) =
      (List(List((2, List(4)), (3, List(15)), (4, List(2))), List((1, List(11)),
        (2, List(6, 23)), (3, List(20))), List((1, List(12)), (2, List(5, 9, 10))), List((3, List(3)))), 0)
    val hyper = new Hyper(matchingWords = 0.8)
    val expected = Set[Int](2, 3)
    val actual = PlagiarismFinder.getRelevantDocuments(indexValues, hyper)
    assert(expected === actual)
  }

  test("getRelevantDocuments") {
    val indexValues: (List[List[(PlagiarismFinder.ID, List[PlagiarismFinder.ArtPos])]], PlagiarismFinder.InPos) =
      (List(List((1, List(10, 22, 23)), (2, List(4)), (3, List(15)), (4, List(2))), List((1, List(11)),
        (2, List(6, 23)), (3, List(20))), List((1, List(12)), (2, List(5, 9, 10))), List((1, List(13)), (3, List(3)))), 0)
    val hyper = new Hyper(matchingWords = 0.7)
    val expected = Set(1, 2, 3)
    val actual = PlagiarismFinder.getRelevantDocuments(indexValues, hyper)
    assert(expected === actual)
  }

  test("filterRelevantDocuments") {
    val indexValues: (List[List[(PlagiarismFinder.ID, List[PlagiarismFinder.ArtPos])]], PlagiarismFinder.InPos) =
      (List(List((1, List(10, 22, 23)), (2, List(4)), (3, List(15)), (4, List(2))), List((1, List(11)),
        (2, List(6, 23)), (3, List(20))), List((1, List(12)), (2, List(5, 9, 10))), List((1, List(13)), (3, List(3)))), 0)
    val groupedValues: (List[(PlagiarismFinder.ID, Iterable[PlagiarismFinder.ArtPos])], PlagiarismFinder.InPos) =
      (List((2, List(4, 5, 6, 9, 10, 23)), (4, List(2)), (1, List(10, 11, 12, 13, 22, 23)), (3, List(3, 15, 20))), 0)
    val hyper = new Hyper()
    val expected = (List((2, List(4, 5, 6, 9, 10, 23)), (1, List(10, 11, 12, 13, 22, 23)), (3, List(3, 15, 20))), 0)
    val actual = PlagiarismFinder.filterRelevantDocuments(groupedValues, indexValues, hyper)
    assert(expected === actual)
  }

  test("filterMaximalDistance") {
    val docs: (List[(PlagiarismFinder.ID, Iterable[PlagiarismFinder.ArtPos])], PlagiarismFinder.InPos) =
      (List((2, List(4, 5, 6, 9, 10, 23)), (1, List(10, 11, 12, 13, 22, 42, 43, 44, 52)), (3, List(3, 15, 20))), 0)
    val hyper = new Hyper(maxDistanceAlpha = 5)
    val expected =
      (List((2, List((4, 0), (5, 1), (6, 1), (9, 3), (10, 1))),
        (1, List((10, 0), (11, 1), (12, 1), (13, 1), (42, 20), (43, 1), (44, 1))),
        (3, List((15, 12), (20, 5)))), 0)
    val actual = PlagiarismFinder.filterMaximalDistance(docs, hyper)
    assert(expected === actual)
  }

  test("computeDistanceBetweenRelevantPositions") {
    val docs: (List[(PlagiarismFinder.ID, List[(PlagiarismFinder.ArtPos, PlagiarismFinder.Delta)])], PlagiarismFinder.InPos) =
      (List((2, List((4, 0), (5, 1), (6, 1), (9, 3), (10, 1))),
        (1, List((10, 0), (11, 1), (12, 1), (13, 1), (42, 20), (43, 1), (44, 1))),
        (3, List((15, 12), (20, 5)))), 0)
    val expected: (List[(PlagiarismFinder.ID, List[(PlagiarismFinder.ArtPos, PlagiarismFinder.Delta)])], PlagiarismFinder.InPos) =
      (List((2, List((4, 0), (5, 1), (6, 1), (9, 3), (10, 1))),
        (1, List((10, 0), (11, 1), (12, 1), (13, 1), (42, 29), (43, 1), (44, 1))),
        (3, List((15, 0), (20, 5)))), 0)
    val actual = PlagiarismFinder.computeDistancesBetweenRelevantPositions(docs)
    assert(expected === actual)
  }

  test("splitIntoRegions") {
    val distances: (List[(PlagiarismFinder.ID, List[(PlagiarismFinder.ArtPos, PlagiarismFinder.Delta)])], PlagiarismFinder.InPos) =
      (List((2, List((4, 0), (5, 1), (6, 1), (12, 6), (13, 1))),
        (1, List((10, 0), (11, 1), (12, 1), (13, 1), (42, 29), (43, 1), (44, 1))),
        (3, List((15, 0), (20, 5)))), 0)
    val hyper = new Hyper(minGroupSize = 3, maxDistanceBeta = 5)
    val expected: (List[(PlagiarismFinder.ID, List[(PlagiarismFinder.ArtPos, PlagiarismFinder.Delta)])], PlagiarismFinder.InPos) =
      (List((2, List((4, 0), (5, 1), (6, 1))),
        (1, List((10, 0), (11, 1), (12, 1), (13, 1))),
        (1, List((42, 29), (43, 1), (44, 1)))), 0)
    val actual = PlagiarismFinder.splitIntoRegions(distances, hyper)

    // The order is irrelevant for the test to be correct
    assert(expected._1.toSet === actual._1.toSet)
  }

  test("calcScore exact") {
    val exact: (List[(PlagiarismFinder.ID, List[(PlagiarismFinder.ArtPos, PlagiarismFinder.Delta)])], PlagiarismFinder.InPos) =
      (List((2, List((4, 0), (5, 1), (6, 1), (7, 1)))), 0)
    val hyper = new Hyper()
    val actual = PlagiarismFinder.calcScore(exact, hyper)._1.head._3
    assert(actual === 1)
  }

  test("calcScore 0.5") {
    val _05match: (List[(PlagiarismFinder.ID, List[(PlagiarismFinder.ArtPos, PlagiarismFinder.Delta)])], PlagiarismFinder.InPos) =
      (List((2, List((4, 0), (6, 2), (8, 2), (10, 2), (10, 2), (12, 2)))), 0)
    val hyper = new Hyper()
    val actual = math.abs(PlagiarismFinder.calcScore(_05match, hyper)._1.head._3 - 0.5)
    assert(actual <= 0.01)
  }

  test("calcScore 0.2") {
    val _02match: (List[(PlagiarismFinder.ID, List[(PlagiarismFinder.ArtPos, PlagiarismFinder.Delta)])], PlagiarismFinder.InPos) =
      (List((2, List((4, 0), (8, 4), (10, 2), (21, 11), (24, 3), (28, 4)))), 0)
    val hyper = new Hyper()
    val actual = math.abs(PlagiarismFinder.calcScore(_02match, hyper)._1.head._3 - 0.2)
    assert(actual <= 0.01)
  }

  test("calcScore 0.0") {
    val _00match: (List[(PlagiarismFinder.ID, List[(PlagiarismFinder.ArtPos, PlagiarismFinder.Delta)])], PlagiarismFinder.InPos) =
      (List((2, List((4, 0), (204, 200), (344, 140), (362, 18), (384, 22), (544, 160)))), 0)
    val hyper = new Hyper()
    val actual = math.abs(PlagiarismFinder.calcScore(_00match, hyper)._1.head._3)
    assert(actual <= 0.01)
  }
}