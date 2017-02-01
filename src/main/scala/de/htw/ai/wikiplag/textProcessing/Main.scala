package de.htw.ai.wikiplag.textProcessing

import de.htw.ai.wikiplag.textProcessing.plagiarism.PlagiarismFinder
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object Main {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("WikiPlagApp")
      .set("spark.default.parallelism", "72")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    PlagiarismFinder(sc, Source.fromInputStream(getClass.getResourceAsStream("/martin.txt")).getLines().reduce(_ + _)).foreach(println)

    sc.stop()
  }
}