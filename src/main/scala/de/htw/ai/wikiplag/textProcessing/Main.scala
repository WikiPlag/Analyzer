package de.htw.ai.wikiplag.textProcessing

import de.htw.ai.wikiplag.textProcessing.plagiarism.PlagiarismFinder

object Main {
  def main(args: Array[String]): Unit = {
    PlagiarismFinder.apply("das ist ein plagiat")
  }
}