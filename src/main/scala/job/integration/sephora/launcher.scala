package job.integration.sephora


import _root_.util.Tools
import fetcher.{s3Fetcher, hbaseFetcher, hdfsFetcher}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.apache.spark.SparkContext._
import scala._
import job.integration.sephora.entity.SepClient
import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.DateTime
import scala.runtime.Nothing$


/**
 * Created with IntelliJ IDEA.
 * User: hadoop
 * Date: 12/19/13
 * Time: 6:13 PM
 */
object launcher {

  val noSQLTable = "base_client_nosql"
  val productTable = "/user/hive/warehouse/t_produit"
  val launchProductDateTable = "/user/hive/warehouse/t_date_sortie_pdt"
  val lastSalesTable = "LIGNES_DERN_VENTE"
  val monthResultProductTable = "/user/hive/warehouse/t_ca_qte_mensuel_pdt"
  val adresseRB = "claravista.data.achats/input/data/RollingBall/Client_RB_anonymes0.csv"

  //date format
  val formatClaraVista: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
  val formatClassic: DateTimeFormatter = DateTimeFormat.forPattern("dd/MM/yyyy")

  //val today = DateTime.now()
  val today = formatClaraVista.parseDateTime("2012-01-01")

  // get raw data result
  //val launchProductDateRDD = hdfsFetcher.readData(launchProductDateTable)

  // deserialize data
  //val launchProductDateLines = launchProductDateRDD.map(_.split("\001")).map{
  //  case Array(idProduct, launchDate) => (idProduct, formatter.parseDateTime(launchDate))
  //}

  def extractPurchaseLines() = {
    val hBaseRDD = hbaseFetcher.readData(noSQLTable)
    val purchaseLines = hBaseRDD.map(SepClient(_).mapPurchaseLines)
    purchaseLines
  }

  def calculateNotesProd() = {

    // get raw data result
    val monthResultProductRDD = hdfsFetcher.readData(monthResultProductTable)
    val productRDD = hdfsFetcher.readData(productTable)
    val lastSalesRDD = hbaseFetcher.readData(lastSalesTable)
    val rbRDD = s3Fetcher.readData(adresseRB)

    // deserialize data
    val monthResultProductLines = monthResultProductRDD.map(_.split("\001")).map{
      case Array(idProduct, month, ca, quantity) => (idProduct, formatClaraVista.parseDateTime(month), ca.toDouble, quantity.toInt)
    }
    val productLines = productRDD.map(_.split("\001")).map{
      case Array(idProduct, materialDescription, materialCreateTMSTP, brandCode, brandDescription, subBrandCode, subBrandDescription, marketCode, marketDescription,
      targetCode, targetDescription, categoryCode, categoryDescription, rangeCode, rangeDescription, natureCode, natureDescription, departmentCode, departmentDescription,
      toExcluded, axe) =>
        (idProduct, materialDescription,if (materialCreateTMSTP == "") "" else formatClassic.parseDateTime(materialCreateTMSTP), brandCode, brandDescription, subBrandCode,
          subBrandDescription, marketCode, marketDescription, targetCode, targetDescription, categoryCode, categoryDescription, rangeCode, rangeDescription, natureCode,
          natureDescription, departmentCode, departmentDescription, toExcluded, axe)
    }
    val lastSalesLines = lastSalesRDD.map(res => (Bytes.toString(res.getRow), res.raw.map(kv => Bytes.toString(kv.getValue)) match {
      case Array(idProduct, idCustomer, lastPurchaseDate) => (idProduct, idCustomer, new DateTime(lastPurchaseDate.toLong * 1000), lastPurchaseDate.toInt)
    } ))
    val rbLines = rbRDD.filter(!_.contains("rb_client")).map(_.split(",")).map{
      case Array(idRB,idCustomer) => (idRB,idCustomer)
    }

    //tables utilisées plusieurs fois
    val caNbCaUnit = monthResultProductLines.filter(x => x._2.isBefore(today) && x._2.isAfter(today.minusYears(3))).groupBy(_._1).map(x => (x._1, x._2.map(x => x._3)
      .sum,  x._2.map(_._4).sum, x._2.map(x => x._3).sum/x._2.map(_._4).sum))

    //fonctions utiles
    def categorization(param: Any, lst: List[Any], start: Int = 0): Any = {
      lst match {
        case Nil => start
        case x :: xs =>  (param,x) match {
          case (param0:Double, x0:Int) => if (param0 > x0) categorization(param0, xs, start + 1) else start
          case (param1:Double, x1:Double) => if (param1 > x1) categorization(param1, xs, start + 1) else start
          case (param2:Int, x2:Double) => if (param2 > x2) categorization(param2, xs, start + 1) else start
          case (param3:Int, x3:Int) => if (param3 > x3) categorization(param3, xs, start + 1) else start
          case _ => null
        }
      }
    }

    def calcContenance(param: String) = {
      param match {
        case a if a.contains("1,5G") => 1.5
        case a if a.contains("1,7GR") => 1.7
        case a if a.contains("2ML") || a.contains("2G") => 2.0
        case a if a.contains("2,5ML") => 2.5
        case a if a.contains("3,5ML") || a.contains("3.5ML") => 3.5
        case a if a.contains("3,7ML") => 3.7
        case a if a.contains("4ML") || a.contains("4 ML") || a.contains("4G") => 4.0
        case a if a.contains("4,4ML") || a.contains("4,4 ML") => 4.4
        case a if a.contains("4,5ML") || a.contains("4,5G") => 4.5
        case a if a.contains("5ML") || a.contains("5 ML") || a.contains("5G") => 5.0
        case a if a.contains("6ML") => 6.0
        case a if a.contains("PERFUM.6.5") => 6.5
        case a if a.contains("7ML") || a.contains("7Mø") => 7.0
        case a if a.contains("7,50ML") || a.contains("7,5ML") || a.contains("7.5ML") || a.contains("7,5Mø") || a.contains("7,5 ML") || a.contains("EDPV 3X7.5")||
          a.contains("EDTV 7,5")|| a.contains("EDT RECH V7,5") || a.contains("VAPO SAC 7.5") || a.contains("V7.5 SAC") || a.contains("EDT 7,5 g") || a.contains("7.5") ||
          a.contains("7,5") => 7.5
        case a if a.contains("8ML") || a.contains("8Mø") => 8.0
        case a if a.contains("9ML") => 9.0
        case a if a.contains("9.5ML") => 9.5
        case a if a.contains("10ML") || a.contains("10 ML") || a.contains("10Mø") || a.contains("10ml") => 10.0
        case a if a.contains("10.2ML") => 10.2
        case a if a.contains("11ML") || a.contains("11Mø") => 11.0
        case a if a.contains("12ML") => 12.0
        case a if a.contains("12,5ML") => 12.5
        case a if a.contains("13ML") => 13.0
        case a if a.contains("15ML") || a.contains("15 ML") || a.contains("VAPO DE SAC 3X15") || a.contains("REFIL15 X2") || a.contains("15Mø") || a.contains("V.15") => 15.0
        case a if a.contains("20ML") || a.contains("20 ML") || a.contains("EDP 20") || a.contains("PERFUM.20") || a.contains("30X20") => 20.0
        case a if a.contains("22ML") || a.contains("22ml") || a.contains("22 ML") => 22.0
        case a if a.contains("25ML") || a.contains("25 ML") || a.contains("25Mø") || a.contains("VAPO25") || a.contains("EDT 25V") || a.contains("25V") => 25.0
        case a if a.contains("27ML") || a.contains("27 ML") || a.contains("EDTV27") => 27.0
        case a if a.contains("28ML") || a.contains("28 ML") || a.contains("EDT 28V") => 28.0
        case a if a.contains("29Mø") => 29.0
        case a if a.contains("RECH.30") || a.contains("RECH.VAPO VOYA.2X30") || a.contains("EDT30") || a.contains("V30 EDT") || a.contains("VAPO 30") || a.contains("EDP 30") ||
          a.contains("EDT 30") || a.contains("30ml") || a.contains("30Mø") || a.contains("30 ML") || a.contains("30ML") || a.contains("EDP30") || a.contains("V30") ||
          a.contains("V.30") || a.contains("C 30") => 30.0
        case a if a.contains("33ML") || a.contains("33Mø") => 33.0
        case a if a.contains("35ML") => 35.0
        case a if a.contains("36ML") => 36.0
        case a if a.contains("40ML") || a.contains("40 ML") || a.contains("EDT40V") => 40.0
        case a if a.contains("45ML") || a.contains("45 ML") => 45.0
        case a if a.contains("47ML") || a.contains("47Mø") => 47.0
        case a if a.contains("EDC50") || a.contains("EDTV50") || a.contains("VAP50") || a.contains("EDP V50") || a.contains("EDTV 50") || a.contains("VAPO50") ||
          a.contains("EAU LEGER V50") || a.contains("EDP V50") || a.contains("VAPO NAT.50") || a.contains("VAPO 50") || a.contains("EDC 50") || a.contains("EDP 50") ||
          a.contains("EDT 50") || a.contains("EDP50") || a.contains("EDT50") || a.contains("5OML") || a.contains("50Mø") || a.contains("50ml") || a.contains("50MØ") ||
          a.contains("50 ml") || a.contains("50 ML") || a.contains("50ML") || a.contains("50VA") || a.contains("50V") || a.contains("V50") || a.contains("ATOM.50") ||
          a.contains("ATO 50") || a.contains("A S 50") || a.contains("A SH 50")  => 50.0
        case a if a.contains("55ML") => 55.0
        case a if a.contains("56ML") => 56.0
        case a if a.contains("57ML") || a.contains("50+7") => 57.0
        case a if a.contains("59ML") => 59.0
        case a if a.contains("60ML") || a.contains("VAP60") || a.contains("60 ML") || a.contains("60Mø") || a.contains("EDT60V") => 60.0
        case a if a.contains("65ML") => 65.0
        case a if a.contains("70ML") => 70.0
        case a if a.contains("75ML") || a.contains("75Mø") || a.contains("75 ML") || a.contains("V75") || a.contains("75 VAPO") || a.contains("EDTV75") ||
          a.contains("EDP 75") || a.contains("EDT 75") || a.contains("VAPO.RECH.75") || a.contains("VAPO 75") || a.contains("VAPO RECH 75") || a.contains("C 75") ||
          a.contains("S 75") || a.contains("EDP75") || a.contains("75ml") || a.contains("TOAL.75") => 75.0
        case a if a.contains("80ML") || a.contains("80V") || a.contains("EP80 REFIL") || a.contains("PERFUM.80") => 80.0
        case a if a.contains("85ML") => 85.0
        case a if a.contains("90ML") || a.contains("90 ML") || a.contains("90Mø") => 90.0
        case a if a.contains("93ML") => 93.0
        case a if a.contains("100ML") || a.contains("100ml") || a.contains("100 ml") || a.contains("100 ML") || a.contains("100Mø") || a.contains("100MØ") ||
          a.contains("100M") || a.contains("EDP100") || a.contains("EDT100") || a.contains("EDTV100") || a.contains("EDTV 100") || a.contains("EDCV 100") ||
          a.contains("EDP 100") || a.contains("EDT 100") || a.contains("VAPO 100") || a.contains("VAPO100") || a.contains("EDT VAPO ED. LIMITED 100") ||
          a.contains("EDTFL 100") || a.contains("EDPV 100") || a.contains("EAU DE TOIL.SS ALC.FL100") || a.contains("EAU DE TOIL.SS ALCOOL 100") || a.contains("EDS 100") ||
          a.contains("EDP LIM.FL.100") || a.contains("100 VAPO") || a.contains("VAPO NAT.100") || a.contains("EDT V100") || a.contains("VAPO RECHARGEABLE 100") ||
          a.contains("PARF.ED.LIM.100") || a.contains("100V") || a.contains("C 100") || a.contains("S 100") || a.contains("V.100") || a.contains("100 L") ||
          a.contains("ATO100") || a.contains("A SH 100") || a.contains("VP100") || a.contains("TOILETTE 100")  => 100.0
        case a if a.contains("109ML") => 109.0
        case a if a.contains("110ML") => 110.0
        case a if a.contains("112ML") || a.contains("112Mø") => 112.0
        case a if a.contains("115ML") => 115.0
        case a if a.contains("116ML") => 116.0
        case a if a.contains("118ML") => 118.0
        case a if a.contains("120ML") || a.contains("120Mø") || a.contains("SPLASH&SPRAY 120") || a.contains("120 ML") => 120.0
        case a if a.contains("125ML") || a.contains("125 ML") || a.contains("125Mø") || a.contains("EDT 125V") || a.contains("EDP125") || a.contains("EDT V125") ||
          a.contains("EDT 125 VAPO") || a.contains("EDT125") || a.contains("TONIQ.CORPS 125") || a.contains("125 VAP") || a.contains("125M") || a.contains("125 ml") ||
          a.contains("TOAL.125") => 125.0
        case a if a.contains("133ML") => 133.0
        case a if a.contains("140ML") => 140.0
        case a if a.contains("150ML") || a.contains("VAPO150") || a.contains("EDT 150M") || a.contains("SS AL 150") => 150.0
        case a if a.contains("175ML") => 175.0
        case a if a.contains("180ML") || a.contains("180 ML") => 180.0
        case a if a.contains("200ML") || a.contains("200Mø") || a.contains("200 ML") || a.contains("200ml") || a.contains("200 LTED")  => 200.0
        case a if a.contains("220ML") => 220.0
        case a if a.contains("225ML") => 225.0
        case a if a.contains("236ML") => 236.0
        case a if a.contains("237ML") => 237.0
        case a if a.contains("240ML") => 240.0
        case a if a.contains("250ML") || a.contains("EDS250") || a.contains("EDT 250") || a.contains("EDTFL 250") => 250.0
        case a if a.contains("300ML") => 300.0
        case a if a.contains("320ML") || a.contains("EDC 320") => 320.0
        case a if a.contains("325ML") => 325.0
        case a if a.contains("350ML") => 350.0
        case a if a.contains("380ML") => 380.0
        case a if a.contains("400ML") || a.contains("400Mø") || a.contains("400 ML") || a.contains("400 LTED") => 400.0
        case a if a.contains("420ML") => 420.0
        case a if a.contains("440ML") => 440.0
        case a if a.contains("450ML") => 450.0
        case a if a.contains("500ML") || a.contains("500 ML") || a.contains("500M") || a.contains("EDT AB/DO 500") || a.contains("FL500") => 500.0
        case a if a.contains("600ML") || a.contains("600Mø") => 600.0
        case a if a.contains("620ML") || a.contains("620 ML") => 620.0
        case a if a.contains("750ML") => 750.0
        case a if a.contains("900ML") => 900.0
        case a if a.contains("1000 ML") || a.contains("1000ML") || a.contains("1000Mø") || a.contains("1L") || a.contains("1 L") => 1000.0
        case a if a.contains("1250ML") || a.contains("1.25 L") => 1250.0
        case a if a.contains("1,3L") => 1300.0
        case a if a.contains("1.5L") || a.contains("1,5L") || a.contains("1500ML") || a.contains("1,5 L") => 1500.0
        case a if a.contains("1584ML") => 1584.0
        case a if a.contains("1650ML") => 1650.0
        case a if a.contains("1700ML") || a.contains("1,7L")  => 1700.0
        case a if a.contains("1800ML") => 1800.0
        case a if a.contains("2 LITRES") || a.contains("2L") || a.contains("2000ML") => 2000.0
        case a if a.contains("2100Mø") => 2100.0
        case a if a.contains("2400ML") => 2400.0
        case a if a.contains("2420ML") => 2420.0
        case a if a.contains("2700ML") => 2700.0
        case a if a.contains("2,8L") || a.contains("2800ML") => 2800.0
        case a if a.contains("3000ML") => 3000.0
        case a if a.contains("3600ML") => 3600.0
        case a if a.contains("3900ML") => 3900.0
        case a if a.contains("7550ML") => 7550.0
        //contenances approximatives
        case a if a.contains("MINIATURE") => 0.0
        case a if a.contains("GEANT") => 1000.0
        //else
        case _=> ""
      }
    }

    val catContenance = List(15, 40, 60, 90, 120, 200, 500)

    def calculNotePrixAbsolu = {
      val catPrixAbs = List(0, 10, 25, 50, 80, 150)
      val notePrixAbsolu  = caNbCaUnit.map(x => (x._1, categorization(x._4, catPrixAbs))).map(x => (x._1, if (x._2 == 0) null else x._2))
      notePrixAbsolu
    }

    def calculNotePrixRelatif() = {

      def catPrixRelatif(axe :String, rangeCode :String, natureCode :String, materialDescription :String, categorieContenance :Any, targetCode :String) = {
        (axe, rangeCode, natureCode,  targetCode) match {
          //BAIN
          case ("BAIN", _, _, _) | ("SOIN", _, "BAINDOUCHE", _) => "BAIN"
          //SOIN
          case ("SOIN", "CAPILLAIRE", _, _) | ("SOIN", "SOLAIRES", "SHAMPOINGS", _) | ("SOIN", "SOLAIRES", "SOINCAPIL", _) |
               ("SOIN", "HYGIENE", "DECOLOR", _) => "SOIN - CAPILLAIRE"
          case ("SOIN", "SOLAIRES", _, _) | ("SOIN", "COFFRET", "SOLAIRE", _) => "SOIN - SOLAIRE"
          case ("SOIN", _, "DEODORANTS", _) | ("SOIN", _, "EPILATIONS", _) | ("SOIN", _, "HYGIENE", _) => "SOIN - HYGIENE"
          case ("SOIN", _, "SOINONGLE", _) => "SOIN - ONGLES"
          case ("SOIN", _, "ANTIAGE", _) | ("SOIN", _, "RAFFERM", _) | ("SOIN", _, "CURE&SERUM", _) | ("SOIN", _, "ECLABLANCH", _) => "SOIN - ANTIAGE"
          case ("SOIN", "CORPS", _, _) | ("SOIN", "COFFRETS", "COFFRETS", _) | ("SOIN", "COFFRET", "DIVERS", _) | ("SOIN", "COFFRET", "EAUDESOIN", _) => "SOIN - CORPS"
          case ("SOIN", "VISAGE", "BASE&CORR", _) | ("SOIN", "VISAGE", "NETTOYANT", _) | ("SOIN", "VISAGE", "RASAGE", _) | ("SOIN", "VISAGE", "DEMAQ", _) |
               ("SOIN", "VISAGE", "APRESRASAG", _) | ("SOIN", "COFFRET", "RASAGE", _) | ("SOIN", "COFFRET", "TRAVEL", _) => "SOIN - VISAGE1"
          case ("SOIN", "VISAGE", "DENTS", _) | ("SOIN", "VISAGE", "TRAITSPEC", _) | ("SOIN", "VISAGE", "MASQUE&GOM", _) | ("SOIN", "VISAGE", "NETTDEMAQ", _) |
               ("SOIN", "VISAGE", "PURIFIANT", _) | ("SOIN", "VISAGE", "SOINQUOTI", _) => "SOIN - VISAGE2"
          case ("SOIN", "VISAGE", "ZONESSPEC", _) | ("SOIN", "VISAGE", "GOMMAGE", _) | ("SOIN", "VISAGE", "TRAITSPE", _) | ("SOIN", "VISAGE", "MASQUE", _) |
               ("SOIN", "VISAGE", "HYDRATANTS", _) | ("SOIN", "COFFRET", "HYDRATANTS", _) => "SOIN - VISAGE3"
          case ("SOIN", "VISAGE", "COFFRETS", _) => "SOIN - COFFRET VISAGE"
          //PARFUM ALCOOLS
          case ("PARFUM", "ALCOOLS", _, _) if materialDescription.contains("EDP") || materialDescription.contains("EAU DE PARF") => "PARFUM - EDP" + categorieContenance
          case ("PARFUM", "ALCOOLS", "EAUPARFUM", _) => "PARFUM - EDP" + categorieContenance
          case ("PARFUM", "ALCOOLS", _, _) if materialDescription.contains("EDT") || materialDescription.contains("EAU DE TOIL") => "PARFUM - EDT" + categorieContenance
          case ("PARFUM", "ALCOOLS", "EAUTOIL", _) => "PARFUM - EDT" + categorieContenance
          case ("PARFUM", "ALCOOLS", _, _) if materialDescription.contains("EDC") => "PARFUM - EDC" + categorieContenance
          case ("PARFUM", "ALCOOLS", "EAUCOLOGNE", _) => "PARFUM - EDC" + categorieContenance
          case ("PARFUM", "ALCOOLS", "EXTRAITS", _) => "PARFUM - EXTRAITS" + categorieContenance
          //PARFUM AUTRES
          case ("PARFUM", "GAMMECOMP", "BAINDOUCHE", _) | ("PARFUM", "GAMMECOMP", "DEODORANTS", _) | ("PARFUM", "GAMMECOMP", "DIVERS", _) |
               ("PARFUM", "GAMMECOMP", "SAVONS", _) => "PARFUM - PDTS DERIVES (DEO, BAIN)"
          case ("PARFUM", "GAMMECOMP", "BOUGIES", _) => ""
          case ("PARFUM", "GAMMECOMP", _, "HOMME") => "PARFUM - PDTS DERIVES HOMMES"
          case ("PARFUM", "GAMMECOMP", _, "FEMME") => "PARFUM - PDTS DERIVES FEMMES"
          case ("PARFUM", "COFFRETS", "MINIATURES", _) => "PARFUM - COFFRETS MINIATURES"
          case ("PARFUM", "COFFRETS", "SAVONS", _) => "PARFUM - COFFRETS SAVONS"
          case ("PARFUM", "COFFRETS", _, "HOMME") => "PARFUM - COFFRETS HOMMES"
          case ("PARFUM", "COFFRETS", _, "FEMME") => "PARFUM - COFFRETS FEMMES"
          //MAQUILLAGE
          case ("MAQUILLAGE", "ONGLES", _, _) | ("MAQUILLAGE", "COFFRETS", "VAO", _) => "MAQUILLAGE - ONGLES"
          case ("MAQUILLAGE", "LEVRES", _, _) if natureCode != "DIVERS" => "MAQUILLAGE - LEVRES"
          case ("MAQUILLAGE", "COFFRETS", "RAL", _) => "MAQUILLAGE - LEVRES"
          case ("MAQUILLAGE", "YEUX", _, _) | ("MAQUILLAGE", "COFFRETS", "FARDAPAUP", _) => "MAQUILLAGE - YEUX"
          case ("MAQUILLAGE", "VISAGE", "DEMAQ", _) | ("MAQUILLAGE", "VISAGE", "PAILLETTES", _) | ("MAQUILLAGE", "VISAGE", "SOINQUOTI", _) |
               ("MAQUILLAGE", "COFFRETS", "PAILLETTES", _) => "MAQUILLAGE - VISAGE1"
          case ("MAQUILLAGE", "VISAGE", "ANTICERNE", _) | ("MAQUILLAGE", "VISAGE", "BASE&CORR", _) | ("MAQUILLAGE", "VISAGE", "BLUSH", _) |
               ("MAQUILLAGE", "VISAGE", "CREMETEINT", _) | ("MAQUILLAGE", "VISAGE", "POUDRES", _) | ("MAQUILLAGE", "VISAGE", "POUDRESOL", _) |
               ("MAQUILLAGE", "VISAGE", "FONDTEINT", _) | ("MAQUILLAGE", "VISAGE", "DIVERS", _) => "MAQUILLAGE - VISAGE2"
          case ("MAQUILLAGE", "COFFRETS", _, _) => "MAQUILLAGE - COFFRETS"
          //ELSE
          case _ => " "
        }

      }

      val categoriePrixRelatif = productLines.map(x => (x._1, catPrixRelatif(x._21, x._14, x._16, x._2, categorization(calcContenance(x._2), catContenance), x._10)))

      val categorieEtCA = categoriePrixRelatif.leftOuterJoin(caNbCaUnit.map(x => (x._1, (x._2, x._3, x._4)))).filter(x => x._2._2.getOrElse((null,0,null))._2 >= 50 & x._2._1 != "" & x._2._1 != null)

      val prixRef = categorieEtCA.map(x => (x._2._1, x._2._2.get._3)).groupByKey().map(x => (x._1,x._2.toArray.sortWith(_<_)))
        .map(x => (x._1, (x._2((x._2.length * 0.2).toInt)+ x._2((x._2.length*0.8).toInt))/2))

      val notePrixRelatif = categorieEtCA.map(x => (x._2._1, (x._1, x._2._2.get._3))).leftOuterJoin(prixRef).filter(_._2._2.get != 0.0)
        .map(x => (x._2._1._1,categorization(x._2._1._2, List(0.64 * x._2._2.get, 0.8 * x._2._2.get, 1.25 * x._2._2.get, 1.56 * x._2._2.get),1)))
      notePrixRelatif
    }


    // Notes parfums alcools
    val notesGestes = productLines.filter(x => x._21 == "PARFUM" && x._14 == "ALCOOLS").map(x => (x._1, Array(
      //EDP
      (x._16,x._2) match {
      case (_,descr) if descr.contains("EDP") => 1
      case (_,descr) if descr.contains("EAU DE PARF") => 1
      case ("EAUPARFUM",_)  => 1
      case _ => 0},
      //EDT
      (x._16,x._2) match {
        case (_,descr) if descr.contains("EDT") => 1
        case (_,descr) if descr.contains("EAU DE TOIL") => 1
        case ("EAUTOIL",_)  => 1
        case _ => 0},
      //EDC
      (x._16,x._2) match {
        case (_,descr) if descr.contains("EDC") => 1
        case ("EAUCOLOGNE",_)  => 1
        case _ => 0},
      //Extraits
      (x._16,x._2) match {
        case ("EXTRAITS",_)  => 1
        case ("GEANT",descr) if descr.contains("EXTRAIT") || descr.contains("EXT.") => 1
        case _ => 0},
      //Vapo Sac
      (x._1,x._2) match {
        case (_,descr) if descr.contains("VAPO SAC") || descr.contains("VAPO DE SAC") || descr.contains("SAC VAPO") || descr.contains("VAPO RECH SAC") ||
          descr.contains("VAPO.RECH.SAC") || descr.contains("VAPO.SAC RECH") || descr.contains("RECH.VAPO.SAC") || descr.contains("VAPO. SAC") || descr.contains("VAPO.SAC") ||
          descr.contains("VAPO. SAC") => 1
        case ("82061",_) | ("134986",_) | ("144452",_) | ("159071",_) => 1
        case _ => 0},
      //Vapo
      x._2 match {
        case descr if !(descr.contains("VAPO SAC") || descr.contains("VAPO DE SAC") || descr.contains("SAC VAPO") || descr.contains("VAPO RECH SAC") ||
          descr.contains("VAPO.RECH.SAC") || descr.contains("VAPO.SAC RECH") || descr.contains("RECH.VAPO.SAC") || descr.contains("VAPO. SAC") || descr.contains("VAPO.SAC") ||
          descr.contains("VAPO. SAC") || descr.contains("EDP VERRE") || descr.contains("EDP VERT") || descr.contains("EDP VIOLET")) &&
          (descr.contains("VAP") || descr.contains("EDPV") || descr.contains("EDTV") || descr.contains("EDT VANILLE") || descr.contains("EDT VANCLEEF") ||
            descr.contains("EDT V") || descr.contains("EDP V")) => 1
        case _ => 0},
      //Flacon
      x._2 match {
        case descr if descr.contains("FL.") || descr.contains("FLACON") || descr.contains("FLAC") => 1
        case _ => 0},
      //Spray
      x._2 match {
        case descr if descr.contains("SPRAY") => 1
        case _ => 0},
      categorization(calcContenance(x._2), catContenance,1)
      ))
    )


    //Notes Parfums
    val notesParfum = productLines.filter(_._21 == "PARFUM").map(x => (x._1, (x._5, x._7) match {
      case ("DIOR","J'ADORE") => 1
      case ("CHANEL","NUMERO 5") => 1
      case ("MUGLER THIERRY","ANGEL") => 1
      case ("KENZO","FLOWER BY KENZO") => 1
      case ("LOLITA LEMPICKA","LOLITA LEMPICKA") => 1
      case ("GIVENCHY","VERY IRRESISTIBLE") => 1
      case ("GUERLAIN","SHALIMAR") => 1
      case ("LANCOME","TRESOR") => 1
      case ("CALVIN KLEIN","CK ONE") => 1
      case ("YVES ST LAURENT","OPIUM") => 1
      case ("J.P. GAULTIER","LE MALE") => 1
      case ("HUGO BOSS","BOSS HOMME") => 1
      case ("ARMANI","ACQUA DI GIO H.") => 1
      case ("DIOR","EAU SAUVAGE") => 1
      case ("GUERLAIN","HABIT ROUGE") => 1
      case _ => 0}
      )
    )


    //Notes Inter-univers
    val notesCrossUnivers = productLines.map(x => (x._1, Array(
      //Seléctif
      if (x._8 == "SELECTIF") 1 else 0,
      //Diffusion
      if (x._8 == "DIFFUSION") 1 else 0,
      //Sephora
      if (x._4 == "SEPHO") 1 else 0,
      //Exclusivité
      x._8 match{
        case "EMERGENT" => 1
        case "SEPHORA" if x._4 != "SEPHO" => 1
        case _ => 0},
      //Edition limitée
      x._2 match{
        case descr if descr.contains("UNLIMITED EDP") => 0
        case descr if descr.contains("ED. LIMIT") || descr.contains("ED.LIMIT") || descr.contains("REEDITION LIMITEE") || descr.contains("ED?LIMITEE") ||
          descr.contains("EDITION LIMITEE") || descr.contains("EDIC.LIMITADA") || descr.contains("E. LIMITADA") || descr.contains("LIMITED ED") ||
          descr.contains("EDITIE LIMITATA") || descr.contains("ED.LIM.") || descr.contains("ED.LI.") || descr.contains("ED. LIM.") || descr.contains("EDLIM") ||
          descr.contains("ED.LIM") || descr.contains("ED. LIM") || descr.contains("EDIT LIM") || descr.contains("EDIT.LIM") || descr.contains("ED LIM") ||
          descr.contains("ED LIMIT") || descr.contains("COLLECTOR") || descr.contains("ED.COLLECT.") => 1
        case _ => 0},
      //Coffret
      if (x._14.contains("COFFRET") || x._16.contains("COFF") || x._2.contains("COFFRE") || x._2.contains("COFF.")) 1 else 0,
      //Mixte
      if (x._10 == "MIXTE") 1 else 0,
      //Homme
      if (x._10 == "HOMME") 1 else 0,
      //Femme
      if (x._10 == "FEMME") 1 else 0,
      //Enfant
      if (x._10 == "ENFANT") 1 else 0,
      //Recharge
      (x._2, x._21) match {
        case (descr,"PARFUM") if descr.contains("RECHGBLE") ||  descr.contains("RECHGEABLE") => 0
        case (descr,_) if descr.contains("RECHARGEABLE") ||  descr.contains("REFILLABLE") ||  descr.contains("NON RECH.") ||  descr.contains("RECHBLE") ||
          descr.contains("RECHARGBLE") ||  descr.contains("RECHGLE") ||  descr.contains("STRECH") ||  descr.contains("FRECH") ||  descr.contains("RECHAUFFE") => 0
        case (descr,_) if descr.contains("REFILL") ||  descr.contains("RECHARGE") ||  descr.contains("RCHG") => 1
        case (descr,"MAQUILLAGE") if descr.contains("RECH") => 1
        case (descr,"SOIN") if descr.contains("RECH") => 1
        case (descr,"PARFUM") if descr.contains("RECHG") => 1
        case _ => 0}
      ))
    )


    val noteRareteSKU = productLines.map(x => (x._1, (x._4, x._8))).leftOuterJoin(caNbCaUnit.map(x => (x._1, x._3))).map(x => (x._1,
      (x._2._1._1,x._2._1._2) match {
        case ("SELECTIF",_) => categorization(x._2._2, List(0,12,60,290,1800,5900))
        case ("DIFFUSION",_) => 6
        case (_,"SEPHO") => categorization(x._2._2, List(0,120,600,2900,18000,59000))
        case ("EMERGENT",_) => categorization(x._2._2, List(0,100,540,2610,16200,53100))
        case _ => null})).map(x => (x._1, if (x._2 ==0) null else x._2)
      )


    val notesMaquillage = productLines.filter(_._21 == "MAQUILLAGE").map( x => (x._1, Array(
      if (x._14 == "YEUX" || (x._14 == "COFFRETS" && (x._16 == "EYELINER" || x._16 == "FARDAPAUP")) || (x._14 == "VISAGE" && x._16 == "ANTICERNE")) 1 else 0,
      if ((x._14 == "VISAGE" && x._16 != "ANTICERNE") || (x._14 == "SOLAIRES" && x._16 == "MAQUILLAGE") ) 1 else 0,
      if (x._14 == "ONGLES" || (x._14 == "COFFRETS" && x._16 == "VAO") ) 1 else 0,
      if (x._14 == "LEVRES" || (x._14 == "COFFRETS" && x._16 == "RAL") ) 1 else 0
      )
      )
    )


    val notesSoin = productLines.filter(_._21 == "SOIN").map( x => (x._1, Array(
      //Zone d'application corps
      (x._14,x._16) match {
        case ("CORPS",_) | ("COFFRET", "AMINCIS") | ("HYGIENE", "BAINDOUCHE") | ("HYGIENE", "DEODORANTS") | ("HYGIENE", "EPILATIONS") | ("HYGIENE", "SAVONS") |
             ("SOLAIRES", "APRESSOL") | ("SOLAIRES", "AUTOBRONZ") | ("SOLAIRES", "BRONZANTS") | ("SOLAIRES", "COFFRETS") | ("SOLAIRES", "RAFRAICHIS") |  ("BAIN B-E", _) => 1
        case _ => 0
      },
      //Zone d'application visage
      (x._14,x._16) match {
        case ("VISAGE",_) | ("ACCESSOIRE", "DEMAQ") | ("COFFRET", "ANTIAGE") | ("COFFRET", "EAUDESOIN") | ("COFFRET", "HYDRATANTS") | ("COFFRET", "RASAGE") |
             ("HYGIENE", "DENTS") |  ("YEUX", _) => 1
        case _ => 0
      },
      //Zone d'application ongles
      x._14 match {
        case ("ONGLES") => 1
        case _ => 0
      },
      //Zone d'application capillaire
      (x._14,x._16) match {
        case ("CAPILLAIRE",_) | ("HYGIENE", "DECOLOR") | ("SOLAIRES", "SHAMPOINGS") | ("SOLAIRES", "SOINCAPIL") => 1
        case _ => 0
      },
      //Besoin/Fonction Hygiène
      (x._14,x._16) match {
        case ("HYGIENE",_) | ("CORPS", "HYGIENE") | ("COFFRET", "RASAGE") | ("VISAGE", "RASAGE") | ("CORPS", "DEODORANTS") | ("VISAGE", "DENTS") => 1
        case _ => 0
      },
      //Besoin/Fonction Capillaire
      (x._14,x._16) match {
        case ("CAPILLAIRE",_) | ("SOLAIRES", "SHAMPOINGS") | ("SOLAIRES", "SOINCAPIL") => 1
        case _ => 0
      },
      //Besoin/Fonction Solaire
      if ((x._14 == "SOLAIRES" && x._16 != "SHAMPOINGS" && x._16 != "SOINCAPIL") || (x._14 == "COFFRET" && x._16 == "SOLAIRE")) 1 else 0,
      //Besoin/Fonction Nettoyage
      (x._14,x._16) match {
        case ("BAIN B-E",_) | ("CORPS", "BAINDOUCHE") | ("VISAGE", "NETTOYANT") | ("VISAGE", "PAPIER MAT") => 1
        case _ => 0
      },
      //Besoin/Fonction Minceur
      (x._14,x._16) match {
        case ("AMINCIS",_) | ("CPLT ALIM", "MINCEUR") | ("CORPS", "RAFFERM") | ("CORPS", "MINCEUR") => 1
        case _ => 0
      },
      //Besoin/Fonction Ongles
      if (x._16 == "SOINONGLE") 1 else 0,
      //Besoin/Fonction Hydratation
      (x._14,x._16) match {
        case ("HYDRATANTS",_) | ("VISAGE", "PURIFIANT") | ("VISAGE", "APRESRASAG") | ("VISAGE", "APRES-RASA") | ("VISAGE", "CREM JOUR") | ("VISAGE", "CREME NUIT") |
             ("VISAGE", "CREMETEINT") | ("VISAGE", "BB CREAM") | ("VISAGE", "LEVRES") => 1
        case _ => 0
      },
      //Besoin/Fonction Anti-âge
      (x._14,x._16) match {
        case ("ANTIAGE",_) | ("CPLT ALIM", "JEUNESSE") | ("VISAGE", "CURE&SERUM") | ("VISAGE", "COMBLEUR") | ("VISAGE", "CREME YEUX") | ("VISAGE", "COU") | ("VISAGE", "SERUM") |
             ("VISAGE", "SERUM YEUX") => 1
        case _ => 0
      },
      //Besoin/Fonction Complément Alimentaire
      if (x._14 == "CPLT ALIM" && x._16 != "JEUNESSE" && x._16 != "MINCEUR") 1 else 0,
      //Besoin/Fonction Démaquillant
      if (x._16.contains("DEMAQ")) 1 else 0,
      //Besoin/Fonction Eau de soin
      x._16 match {
        case "EAUDESOIN" | "TONIQCORPS" | "TONIQUE" | "YEUX TONIQ" | "BRUME" => 1
        case _ => 0
      },
      //Besoin/Fonction Masque/Gommage
      x._16 match {
      case "GOMMAGE" | "MASQUE" | "MASQUE&GOM" | "MASQ CRGEL" | "MASQ TISSU" | "MASQ YEUX" | "EXFOLIANT" => 1
      case _ => 0
      },
      //Besoin/Fonction Soin Quotidien
      (x._14,x._16) match {
      case ("SOINQUOTI",_) | ("VISAGE", "LOTION GEL") => 1
      case _ => 0
      },
      //Besoin/Fonction Traitement Spécifique
      if (x._16 == "ECLABLANCH" || x._16 == "TRAITSPE" ||x._16 == "TRAITSPEC" || (x._16 == "BAS&CORR" && x._6 == "SEPHO51") ) 1 else 0,
      //Besoin/Fonction Marque Emergente
      if (x._16 == "ZONESSPEC") 1 else 0
      )
      )
    )


    def calculNotesSubjectives = {


      val product4Timer = productLines.filter(x => x._1 != "63301" && x._4 != "ECHAN" && x._4 != "PLV" && x._5 != "FIDELITE" && x._6 != "SEPHO60" && x._14 != "BAIN B-E")
        .map(x => (x._1, true))

      val clients4Timer = extractPurchaseLines().filter(x => x._6.length > 4 && x._6.map(_._3.map(y => y._2 * y._3).sum).sum > 0.0 && Tools.concatener(x._6.map(_._3.map(_._1)))
        .distinct.length > 4 && (x._4 == 2 || x._4 == 3 || x._4 == 4) ).map(x => (x._1, true))

      //ATTENTION!!!!!!!!!!!!!!!!!!!!!!!!!!!!! premier filter temporaire pour éviter Java Heap Space
      val lastSales4TimerLines = lastSalesLines.filter(_._2._3.getMonthOfYear > 11).map(x => (x._2._1, x._2._2)).leftOuterJoin(product4Timer).filter(_._2._2.getOrElse(false)).map(x => (x._2._1 , x._1))
        .leftOuterJoin(clients4Timer).filter(_._2._2.getOrElse(false)).map(x => (x._1, x._2._1)).leftOuterJoin(rbLines.map(x => (x._2,x._1))).map(x => (x._1, x._2._1, x._2._2.getOrElse(null)))


      val nb_client_tot = lastSales4TimerLines.map(_._1).distinct.count
      val nb_client_rb = rbLines.countByKey

      println("-----------------------------------------------------")
      println(nb_client_tot)
      println("-----------------------------------------------------")

      val notesSubjectives = lastSales4TimerLines.map(x => (x._2, x._3)).groupBy(_._1)
        .map(x => (x._1, x._2.toArray.groupBy(_._2).filter(z => nb_client_rb.contains(z._1))
        .map(y => if(nb_client_rb.get(y._1).getOrElse(0) == 0 || x._2.length == 0 || nb_client_tot == 0) null else  (y._2.length.toDouble / nb_client_rb.get(y._1).get.toDouble) / (x._2.length.toDouble / nb_client_tot.toDouble))))
      notesSubjectives

    }

    val notesProduit = notesCrossUnivers
      .leftOuterJoin(notesSoin).map(x => (x._1, x._2._2.getOrElse(null) +: x._2._1))
      .leftOuterJoin(notesMaquillage).map(x => (x._1, x._2._2.getOrElse(null) +: x._2._1))
      .leftOuterJoin(noteRareteSKU).map(x => (x._1, x._2._2.getOrElse(null) +: x._2._1))
      .leftOuterJoin(notesParfum).map(x => (x._1, x._2._2.getOrElse(null) +: x._2._1))
      .leftOuterJoin(notesGestes).map(x => (x._1, x._2._2.getOrElse(null) +: x._2._1))
      .leftOuterJoin(calculNotePrixAbsolu).map(x => (x._1, x._2._2.getOrElse(null) +: x._2._1))
      .leftOuterJoin(calculNotePrixRelatif).map(x => (x._1, x._2._2.getOrElse(null) +: x._2._1))
      .leftOuterJoin(calculNotesSubjectives).map(x => (x._1, x._2._2.getOrElse(null) +: x._2._1))


    val test =productLines.count()

    val test0 = notesProduit.count()

    val test1 = notesProduit.first()._2

    println("------------------------------------")

    println(test0)

    println("------------------------------------")

    println(test)

    println("------------------------------------")

    println(test1(0) + " ; " + test1(1) + " ; " + test1(2) + " ; " + test1(3) + " ; " + test1(4) + " ; " + test1(5) + " ; " + test1(6) + " ; " +test1(7) + " ; " + test1(8) + " ; " +test1(9) + " ; " + test1(10) + " ; " +test1(11) + " ; " +test1(12))

    println("------------------------------------")
    notesProduit
  }

  def calculateNotesClient() = {

    val temp = extractPurchaseLines().map(x => (x._1,Tools.concatener(x._6.map(_._3.map(y => (y._1, y._2 * y._3))))))

    val temp2 = temp.map(x => (x._1, calculateNotesProd.filter(_._1 == x._2.head).first()._2  +:  x._2.tail match {
      case a :List[(Double, Array[Any])] => a
      case _ => throw new Exception
    }))

    def productArray(param :List[(Double, Array[Any])]) : Array[Double] = {
      param match {
        case x:: xs => x._2.map(y => y match {
          case d:Double => d * x._1
          case _ => 0.0
        }).zip(productArray(xs)).map(z => z._1 + z._2)
        case _ => new Array[Double](param.head._2.length)
      }
    }


    def sommeCA(param :List[(Double, Array[Any])]) : Array[Double] = {
       param match {
         case x:: xs => x._2.map(y => if (y.isInstanceOf[Double]) x._1 else 0.0).zip(sommeCA(xs)).map(z => z._1 + z._2)
         case _ => new Array[Double](param.head._2.length)
       }
    }



    //val temp3 = temp2.map(x => (x._1, x._2.map(_._2.map())))


  }


  def run() = {

    calculateNotesClient

    println
    println("blabla")
    println

  }

}

