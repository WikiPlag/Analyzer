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
    lazy val raw = Stream(
      ( "Duo il publicate extrahite, toto movimento denomination que de. O del integre movimento. " +
        "Web lo nomina active vocabulario, su sed scientia involvite summarios. Unic peano sia tu, " +
        "que tres ultra avantiate ma. Campo giuseppe web de. Qui da prime spatios. Philologos " +
        "anglo-romanic sia se, post medio questiones le uno, clave parolas ha per. Inviar africa parola qui da. " +
        "Pan scriber integre denominator al. Philologos independente qui se, vices publicava utilitate pro da. " +
        "Lo via russo abstracte traduction, pan ma campo latino. Debe concretisation in pan. " +
        "De active national effortio del, cadeva instruite de qui.", 0 ),
      ( "Sed spatios questiones ha, basate publicate utilitate lo que. Tu del original quotidian, " +
        "sed duce latino immediatemente o. Al sed anque europa. Historia articulo traduction il que, " +
        "medio ascoltar anglo-romanic per de, al malo promotores connectiones nos. Lo sia americas international. " +
        "Flexione professional uso ma, un pan duce disuso europee. Resultato auxiliary association su uno. " +
        "Existe voluntate que ha, ha uso veni texto intermediari, sine spatios abstracte tu non. Del al multo scriber. " +
        "Pan su ample message introductori.", 1 ),
      ( "Se iala prime representantes duo. In clave publicate publicationes nos, su major preparation con. " +
        "Duce human per de. Al membros integre duo, via al veni presenta concretisation. " +
        "Qui human hodie interlingua es, se pan asia post populos.", 2 ),
      ( "Pan post lateres interlingua ma. Per hodie auxiliar initialmente se, le complete traducite que. " +
        "Ille original pan un, non tempore primarimente in. Americano instruite duo o, il lista human debitores nos. " +
        "Uno texto existe romanic ha, con clave studio avantiate lo, su nos post apprende representantes.", 3 )
    )
  }

  test("index contains right number of keys") {
    val actual =
  }
}
