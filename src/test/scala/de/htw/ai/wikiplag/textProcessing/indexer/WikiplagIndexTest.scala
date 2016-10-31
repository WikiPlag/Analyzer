package de.htw.ai.wikiplag.textProcessing.indexer

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * Created by kuro on 10/31/16.
  */
@RunWith(classOf[JUnitRunner])
class WikiplagIndexTest extends FunSuite {
  trait TestIndexer {
    lazy val index = WikiplagIndex
  }

  test("index contains right number of keys") {

  }
}
