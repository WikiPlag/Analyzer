package de.htw.ai.wikiplag.textProcessing

/**
  * Created by _ on 11/2/16.
  */

object Tokenizer {

  val stopwords=Set("") //muss noch ergänzt werden
  //val split_regex="\\W+" //zahlen werden halt noch nicht rausgeschmissen, sollte man vielleicht
  val split_regex="[^\\p{L}]" //zahlen werden halt noch nicht rausgeschmissen, sollte man vielleicht

  def tokenizeString(s:String):List[String]={

    val words= s.toLowerCase.split(split_regex).toList
    words.filter(_!="")
  }

  def tokenize(s:String):List[String]= {
    /*
   	* Tokenize splits the String in to single words
   	* and deletes all stop words.
   	* Use Utils.tokenizeString to split the string
   	*/

    tokenizeString(s).filter(!stopwords.contains(_))
  }
}
