package de.htw.ai.wikiplag.textProcessing

import de.htw.ai.wikiplag.textProcessing.plagiarism.PlagiarismFinder

object Main {

  def main(args: Array[String]): Unit = {
    println("Hello, world!")

    //Hyperparameter
    val h_textSplitLength = 4 //if text should be one single size set h_textSplitLength="Number of Tokens" and h_textSplitStep=1
    val h_textSplitStep = 1

    //Slice Full Text into Equal Parts
    val testText = "das ist ein plagiat"
    val textParts = PlagiarismFinder.splitText(testText, h_textSplitLength, h_textSplitStep)
    for (part <- textParts) println(part)

    //val testMap = Map(("wort1",(1,8)),("wort2",(3,9)))
    //for (entry <- testMap) println(entry)
    println()
    for (part <- textParts) PlagiarismFinder.checkForPlagiarism(part)

    /*
    val testMap = Map(("hello",List((BigInt(1),2))),("world",List((BigInt(3),4))))
    val keyList = List("hello","world")
    val erg = keyList.collect(testMap)
    println(erg)*/
  }


}