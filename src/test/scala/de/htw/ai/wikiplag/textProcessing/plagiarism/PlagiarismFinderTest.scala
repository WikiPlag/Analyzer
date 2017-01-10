package de.htw.ai.wikiplag.textProcessing.plagiarism

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


/**
  * Created by kapoor on 06.06.2016.
  */

@RunWith(classOf[JUnitRunner])
class PlagiarismFinderTest extends FunSuite {
/*
  test("splitText empty") {
    val text = ""
    val splitLength = 5
    val stepSize = 2
    val expected = List()
    val actual = PlagiarismFinder.splitText(text, splitLength, stepSize)
    assert(expected === actual)
  }*/
/*
  test("splitText (5, 2)") {
    val text = "Ich bin ein ganz gewöhnlicher Text! Nicht mehr und nicht weniger"
    val splitLength = 5
    val stepSize = 2
    val expected = List(
      List("ich", "bin", "ein", "ganz", "gewöhnlicher"),
      List("ein", "ganz", "gewöhnlicher", "text", "nicht"),
      List("gewöhnlicher", "text", "nicht", "mehr", "und"),
      List("nicht", "mehr", "und", "nicht", "weniger"))
    val actual = PlagiarismFinder.splitText(text, splitLength, stepSize)
    assert(expected === actual)
  }*/
/*
  test("groupTokens empty") {
    val text = ""
    val expected = Map()
    val actual = PlagiarismFinder.groupTokens(List())
    assert(expected === actual)
  }

  test("groupTokens single") {
    val text = "Ich bin ein ganz gewöhnlicher Text!"
    val expected = Map("ich" -> 1, "bin" -> 1, "ein" -> 1, "ganz" -> 1, "gewöhnlicher" -> 1, "text" -> 1)
    val actual = PlagiarismFinder.groupTokens(List("ich", "bin", "ein", "ganz", "gewöhnlicher", "text"))
    assert(expected === actual)
  }

  test("groupTokens multi") {
    val text = "Ich bin ein ganz gewöhnlicher Text! Nicht mehr und nicht weniger! Nicht doch Text!"
    val expected = Map(
      "ich" -> 1, "bin" -> 1, "ein" -> 1, "ganz" -> 1, "gewöhnlicher" -> 1, "text" -> 2,
      "nicht" -> 3, "mehr" -> 1, "und" -> 1, "weniger" -> 1, "doch" -> 1
    )
    val actual = PlagiarismFinder.groupTokens(
      List("ich", "bin", "ein", "ganz", "gewöhnlicher", "text", "nicht",
           "mehr", "und", "nicht", "weniger", "nicht", "doch", "text")
    )
    assert(expected === actual)
  }
*/
}