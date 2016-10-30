package test.scala.de.htw.ai.wikiplag.parser

import main.scala.de.htw.ai.wikiplag.parser.WikiDumpParser
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


/**
  * Created by kapoor on 06.06.2016.
  */

@RunWith(classOf[JUnitRunner])
class ParserTest extends FunSuite {

  trait TestParser {
    val parser = WikiDumpParser
  }

  /**
    *
    */
  test("removeMatchWithGroup groupId=2") {
    new TestParser {
      val text = "<test>word1</test>"
      val regexList = List("""(?s)<test(>| .*?>)(.*?)</test>""".r)
      val groupId = 2
      val expected = "word1"
      val result = parser.removeMatchWithGroup(text, regexList, groupId)

      assert(expected === result)
    }
  }

  test("removeMatchWithGroup groupId out of range") {
    new TestParser {
      val text = "<test>word1</test>"
      val regexList = List("""(?s)<test(>| .*?>)(.*?)</test>""".r)
      val groupId = 6
      val expected = "6"
      val result = intercept[ArrayIndexOutOfBoundsException] {
        parser.removeMatchWithGroup(text, regexList, groupId)
      }

      assert(expected === result.getMessage)
    }
  }

  test("removeMatchWithGroup empty regexList") {
    new TestParser {
      val text = "<test>word1</test>"
      val regexList = List()
      val groupId = 2
      val expected = "<test>word1</test>"
      val result = parser.removeMatchWithGroup(text, regexList, groupId)

      assert(expected === result)
    }
  }

  test("removeMatchWithGroup regexList=null") {
    new TestParser {
      val text = "<test>word1</test>"
      val regexList = null
      val groupId = 2
      val expected = null
      val result = intercept[NullPointerException] {
        parser.removeMatchWithGroup(text, regexList, groupId)
      }
      assert(result.getMessage === expected)
    }
  }

  test("removeMatchWithGroup empty page content") {
    new TestParser {
      val text = ""
      val regexList = List("""(?s)<test(>| .*?>)(.*?)</test>""".r)
      val groupId = 2
      val expected = ""
      val result = parser.removeMatchWithGroup(text, regexList, groupId)

      assert(expected === result)
    }
  }

  test("removeMatchWithGroup page content=null") {
    new TestParser {
      val text = null
      val regexList = List("""(?s)<test(>| .*?>)(.*?)</test>""".r)
      val groupId = 2
      val expected = null
      val result = intercept[NullPointerException] {
        parser.removeMatchWithGroup(text, regexList, groupId)
      }
      assert(result.getMessage === expected)
    }
  }

  test("removeMatchWithGroup nested tag") {
    new TestParser {
      val text = "<test>word1<test>word2</test></test>"
      val regexList = List("""(?s)<test(>| .*?>)(.*?)</test>""".r)
      val groupId = 2
      val expected = "word1<test>word2</test>"
      val result = parser.removeMatchWithGroup(text, regexList, groupId)

      assert(expected === result)
    }
  }

  test("removeMatchWithGroup escaped Char $") {
    new TestParser {
      val text = "<test>word1$</test>"
      val regexList = List("""(?s)<test(>| .*?>)(.*?)</test>""".r)
      val groupId = 2
      val expected = "word1$"
      val result = parser.removeMatchWithGroup(text, regexList, groupId)

      assert(expected === result)
    }
  }

  /**
    *
    */
  test("replaceNestedTags simple tag") {
    new TestParser {
      val text = "<test>word1</test>"
      val regexAsString = "<test"
      val regex = """(?s)<test(>| .*?>)(.*?)</test>""".r
      val expected = "word1"
      val result = parser.replaceNestedTags(text, regexAsString, regex)

      assert(expected === result)
    }
  }

  test("replaceNestedTags nested tags") {
    new TestParser {
      val text = "<test>word1 <test>word2</test></test>"
      val regexAsString = "<test"
      val regex = """(?s)<test(>| .*?>)(.*?)</test>""".r
      val expected = "word1 word2"
      val result = parser.replaceNestedTags(text, regexAsString, regex)

      assert(expected === result)
    }
  }

  test("replaceNestedTags nested tags new lines and carriage returns") {
    new TestParser {
      val text = "<test>word1 \r<test>word2\n\r</test>\n</test>"
      val regexAsString = "<test"
      val regex = """(?s)<test(>| .*?>)(.*?)</test>""".r
      val expected = "word1 \rword2\n\r\n"
      val result = parser.replaceNestedTags(text, regexAsString, regex)

      assert(expected === result)
    }
  }

  test("replaceNestedTags ignore broken nested tags") {
    new TestParser {
      val text = "<test>word1 <test>word2</test>"
      val regexAsString = "<test"
      val regex = """(?s)<test(>| .*?>)(.*?)</test>""".r
      val expected = "<test>word1 word2"
      val result = parser.replaceNestedTags(text, regexAsString, regex)

      assert(expected === result)
    }
  }

  test("replaceNestedTags tagAsRegex=null") {
    new TestParser {
      val text = "<test>word1 <test>word2</test>"
      val regexAsString = "<test"
      val regex = null
      val expected = null
      val result = intercept[NullPointerException] {
        parser.replaceNestedTags(text, regexAsString, regex)
      }

      assert(result.getMessage === expected)
    }
  }

  test("replaceNestedTags tag=null") {
    new TestParser {
      val text = "<test>word1 <test>word2</test>"
      val regexAsString = null
      val regex = """(?s)<test(>| .*?>)(.*?)</test>""".r
      val expected = null
      val result = intercept[NullPointerException] {
        parser.replaceNestedTags(text, regexAsString, regex)
      }

      assert(result.getMessage === expected)
    }
  }

  ignore("replaceNestedTags empty page content empty regex") {
    new TestParser {
      // inf loop
      assert("" === parser.replaceNestedTags("", "", """(?s)<small(>| .*?>)(.*?)</small>""".r))
    }
  }

  /**
    *
    */
  test("extractPlainText") {
    new TestParser {
      val text = "Alan Smithee steht als Pseudonym für einen fiktiven Regisseur, der Filme verantwortet, " +
        "bei denen der eigentliche Regisseur seinen Namen nicht mit dem Werk in Verbindung gebracht haben möchte. " +
        "Von 1968 bis 2000 wurde es von der Directors Guild of America (DGA) für solche Situationen empfohlen, " +
        "seither ist es Thomas Lee. Alan Smithee ist jedoch weiterhin in Gebrauch."
      val expected = List(
        "Alan", "Smithee", "steht", "als", "Pseudonym", "für", "einen", "fiktiven", "Regisseur", "der", "Filme",
        "verantwortet", "bei", "denen", "der", "eigentliche", "Regisseur", "seinen", "Namen", "nicht", "mit",
        "dem", "Werk", "in", "Verbindung", "gebracht", "haben", "möchte", "Von", "bis", "wurde", "es", "von",
        "der", "Directors", "Guild", "of", "America", "DGA", "für", "solche", "Situationen", "empfohlen",
        "seither", "ist", "es", "Thomas", "Lee", "Alan", "Smithee", "ist", "jedoch", "weiterhin", "in", "Gebrauch"
      )
      val result = parser.extractPlainText(text)

      assert(expected === result)
    }
  }

  test("extractPlainText unicode") {
    new TestParser {
      val text = "Ä Ü Ö Ελλάδα Elláda, formell Ελλάς, Ellás ‚Hellas‘; amtliche Vollform Ελληνική Δημοκρατία, Ellinikí"
      val expected = List(
        "Ä", "Ü", "Ö", "Ελλάδα", "Elláda", "formell", "Ελλάς", "Ellás", "Hellas", "amtliche", "Vollform", "Ελληνική",
        "Δημοκρατία", "Ellinikí"
      )
      val result = parser.extractPlainText(text)

      assert(expected === result)
    }
  }

  test("extractPlainText page content=null") {
    new TestParser {
      val text = null
      val expected = null
      val result = intercept[NullPointerException] {
        parser.extractPlainText(text)
      }
      assert(result.getMessage === expected)
    }
  }

  test("extractPlainText empty page content") {
    new TestParser {
      val text = ""
      val expected = List()
      val result = parser.extractPlainText("")
      assert(expected === result)
    }
  }

  /**
    *
    */
  test("SMALL_TAG_PATTERN") {
    new TestParser {
      val text = "<small>word1</small>"
      val expected = List("<small>word1</small>")
      val result = parser.SMALL_TAG_PATTERN.findAllMatchIn(text).toList.map(_.toString())

      assert(expected === result)
    }
  }

  test("SMALL_TAG_PATTERN carriage returns and new lines") {
    new TestParser {
      val text = "<small>\rword1\r\nword2\n</small>"
      val expected = List("<small>\rword1\r\nword2\n</small>")
      val result = parser.SMALL_TAG_PATTERN.findAllMatchIn(text).toList.map(_.toString())

      assert(expected === result)
    }
  }

  test("SMALL_TAG_PATTERN reluctant") {
    new TestParser {
      val text = "<small>word1</small><small>word1</small>"
      val expected = List("<small>word1</small>", "<small>word1</small>")
      val result = parser.SMALL_TAG_PATTERN.findAllMatchIn(text).toList.map(_.toString())

      assert(expected === result)
    }
  }

  test("SMALL_TAG_PATTERN with tag attribute") {
    new TestParser {
      val text = "<small data=\"test\">word1</small>"
      val expected = List("<small data=\"test\">word1</small>")
      val result = parser.SMALL_TAG_PATTERN.findAllMatchIn(text).toList.map(_.toString())

      assert(expected === result)
    }
  }

  test("SMALL_TAG_PATTERN with tag attributes") {
    new TestParser {
      val text = "<small data=\"test\" data=\"test2\">word1</small>"
      val expected = List("<small data=\"test\" data=\"test2\">word1</small>")
      val result = parser.SMALL_TAG_PATTERN.findAllMatchIn(text).toList.map(_.toString())

      assert(expected === result)
    }
  }

  test("test SMALL_TAG_PATTERN tag inside tag") {
    new TestParser {
      val text = "<small><tag>word1</tag></small>"
      val expected = List("<small><tag>word1</tag></small>")
      val result = parser.SMALL_TAG_PATTERN.findAllMatchIn(text).toList.map(_.toString())

      assert(expected === result)
    }
  }

  test("SMALL_TAG_PATTERN tag inside tag get content") {
    new TestParser {
      val text = "<small><tag>word1</tag></small>"
      val expected = "<tag>word1</tag>"
      val result = parser.SMALL_TAG_PATTERN.findAllMatchIn(text).next().group(2)

      assert(expected === result)
    }
  }

  test("SMALL_TAG_PATTERN with tag attribute get content") {
    new TestParser {
      val text = "<small data=\"test\">word1</small>"
      val expected = "word1"
      val result = parser.SMALL_TAG_PATTERN.findAllMatchIn(text).next().group(2)

      assert(expected === result)
    }
  }

  test("SMALL_TAG_PATTERN get content") {
    new TestParser {
      val text = "<small>word1</small>"
      val expected = "word1"
      val result = parser.SMALL_TAG_PATTERN.findAllMatchIn(text).next().group(2)

      assert(expected === result)
    }
  }

  test("SMALL_TAG_PATTERN carriage returns and new lines get content") {
    new TestParser {
      val text = "<small>\rword1\r\nword2\n</small>"
      val expected = "\rword1\r\nword2\n"
      val result = parser.SMALL_TAG_PATTERN.findAllMatchIn(text).next().group(2)

      assert(expected === result)
    }
  }

  /**
    *
    */
  test("removeWikiMarkup") {
    new TestParser {
      val text = "*\n* \n* ,.()*\n* Listelement\n*text"
      val regex = """^\*[^A-Za-z0-9]{0,}$""".r
      val expected = "\n\n\n* Listelement\n*text"
      val result = parser.removeWikiMarkup(text, List(regex))

      assert(expected === result)
    }
  }

  test("removeWikiMarkup text=null") {
    new TestParser {
      val text = null
      val regex = """^\*[^A-Za-z0-9]{0,}$""".r
      val expected = null
      val result = intercept[NullPointerException] {
        parser.removeWikiMarkup(text, List(regex))
      }
      assert(result.getMessage === expected)
    }
  }

  test("removeWikiMarkup regex=null") {
    new TestParser {
      val text = "text"
      val regex = null
      val expected = null
      val result = intercept[NullPointerException] {
        parser.removeWikiMarkup(text, regex)
      }

      assert(result.getMessage === expected)
    }
  }

  /**
    *
    */
  test("parseXMLWikiPage") {
    new TestParser {
      val text = "1997 kam \'\'die\'\' Parodie An [[Alan Smithee Film]]: Burn Hollywood"
      val expected = "1997 kam die Parodie An Alan Smithee Film: Burn Hollywood"
      val result = parser.parseXMLWikiPage(text)

      assert(expected === result)
    }
  }

  test("parseXMLWikiPage escape html") {
    new TestParser {
      val text = "&nbsp;&apos;&amp;&gt;&pound;&sect;&AElig;"
      val expected = " '&>£§Æ"
      val result = parser.parseXMLWikiPage(text)

      assert(expected === result)
    }
  }

  test("parseXMLWikiPage remove templates") {
    new TestParser {
      val text = "1997 kam die Parodie An Alan {{Text {{Text}} }} Smithee Film: Burn Hollywood"
      val expected = "1997 kam die Parodie An Alan |TEMPLATE| Smithee Film: Burn Hollywood"
      val result = parser.parseXMLWikiPage(text)

      assert(expected === result)
    }
  }

  test("parseXMLWikiPage remove templates new lines") {
    new TestParser {
      val text = "1997 kam die Parodie An Alan {{Text {{Text\n}} \n}} Smithee Film: Burn Hollywood"
      val expected = "1997 kam die Parodie An Alan |TEMPLATE| Smithee Film: Burn Hollywood"
      val result = parser.parseXMLWikiPage(text)

      assert(expected === result)
    }
  }

  test("removeExternalLinks remove all links") {
    new TestParser {
      val text = "[[Helmut Willems]]&lt;ref&gt;[http://www.uni-bielefeld.de/ikg/wissensaustausch/" +
        "wissenschaftler_willems.htm Forschungsverbund Desintegration: Projektleiter: Dr. Helmut Willems]&lt;/ref&gt;"
      val expected = "[[Helmut Willems]]&lt;ref&gt;Forschungsverbund Desintegration: " +
        "Projektleiter: Dr. Helmut Willems&lt;/ref&gt;"
      val result = parser.removeExternalLinks(text)

      assert(expected === result)
    }
  }
}