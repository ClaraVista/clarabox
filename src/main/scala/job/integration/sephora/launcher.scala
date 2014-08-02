package job.integration.sephora


import _root_.util.Tools
import fetcher.{csvFetcher, s3Fetcher, hbaseFetcher, hdfsFetcher}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.apache.spark.SparkContext._
import scala._
import job.integration.sephora.entity.SepClient
import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.{Days, Months, DateTime}
import job.integration.sephora.tablesFetcher.{Tables_Constantes, TABLE_PRODUIT_POLOGNE_VF_Fetcher, CA_QTE_PDT_DATE_POLOGNE_Fetcher}
import job.integration.sephora.calculNotes.Calcul_Notes_Constantes
import job.integration.sephora.tools.StringToDate


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

  /**
   * Récupère la liste des lignes de vente
   * @return Un RDD contenant les lignes de ventes. Chaque ligne du RDD est un tuple de 6 paramètres,
   *         les paramètres sont :
   *         (idClient, idGender, age, idCardStatus, idSegmentRFM, List[(idTicket, pruchaseDate, List[(idProduct, quantity, price)])])
   */
   def extractPurchaseLines() = {

    //Récupère la table noSQLTable (="base_client_nosql"), obtient un RDD[Result]
    val hBaseRDD = hbaseFetcher.readData(noSQLTable)

    //Pour chaque ligne, crée un "client" qui permet d'obtenir la ligne avec ses achats
    //La ligne est un tuple de 6 paramètres
    val purchaseLines = hBaseRDD.map(SepClient(_).mapPurchaseLines)
    purchaseLines
  }

  /**
   * Renvoie l'indice du premier élément de la liste supérieur ou égal à param.
   * Ne touche pas à la liste, elle doit donc avoir été triée avant
   * @param param : l'élément
   * @param lst : la liste
   * @param start : parametre optionnel, utiliser en interne pour rendre la fonction tail recursive
   * @return L'indice du premier élément de la liste supérieur ou égal à param (Si jamais il y a une erreur, renvoit -1)
   */
  def categorization(param: Any, lst: List[Any], start: Int = 0): Int = {
    lst match {
      case Nil => start
      case x :: xs =>  (param,x) match {
        case (param0:Double, x0:Int) => if (param0 > x0) categorization(param0, xs, start + 1) else start
        case (param1:Double, x1:Double) => if (param1 > x1) categorization(param1, xs, start + 1) else start
        case (param2:Int, x2:Double) => if (param2 > x2) categorization(param2, xs, start + 1) else start
        case (param3:Int, x3:Int) => if (param3 > x3) categorization(param3, xs, start + 1) else start
        case _ => -1
      }
    }
  }


  def remove_contenance_ml_material_description(m_desc:String):String = {
    val reg = """[0-9]+[.,]*[0-9]*[mM][lL]""".r
    reg.replaceAllIn(m_desc, "").trim
  }
  /**
   * Calcul les notes produits
   * @return Un RDD[(idProduct, Array(flags et autres)]
   */
  def calculateNotesProd() = {



    // get raw data result

    val monthResultProductRDD = hdfsFetcher.readData(monthResultProductTable)
    val productRDD = hdfsFetcher.readData(productTable)
    val lastSalesRDD = hbaseFetcher.readData(lastSalesTable)
    val rbRDD = s3Fetcher.readData(adresseRB)

    // deserialize data

    //monthResultProductLines = RDD[(idProduct (String), month (Datetime), CA (Double), Quantity (Int))]
    val monthResultProductLines = monthResultProductRDD.map(_.split("\001")).map{
      case Array(idProduct, month, ca, quantity) => (idProduct, formatClaraVista.parseDateTime(month), ca.toDouble, quantity.toInt)
    }

    //RDD des produit, tuple de 21 paramètres
    //x._1 : idProduct
    //x._2 : materialDescription
    //x._3 : if (materialCreateTMSTP == "") "" else formatClassic.parseDateTime(materialCreateTMSTP)
    //x._4 : brandCode,
    //x._5 :brandDescription
    //x._6 : subBrandCode
    //x._7 : subBrandDescription
    //x._8 : marketCode
    //x._9 : marketDescription
    //x._10 : targetCode
    //x._11 : targetDescription
    //x._12 : categoryCode
    //x._13 : categoryDescription
    //x._14 : rangeCode
    //x._15 : rangeDescription
    //x._16 : natureCode
    //x._17 : natureDescription
    //x._18 : departmentCode
    //x._19 : departmentDescription
    //x._20 : toExcluded
    //x._21 : axe
    val productLines = productRDD.map(_.split("\001")).map{
      case Array(idProduct, materialDescription, materialCreateTMSTP, brandCode, brandDescription, subBrandCode, subBrandDescription, marketCode, marketDescription,
      targetCode, targetDescription, categoryCode, categoryDescription, rangeCode, rangeDescription, natureCode, natureDescription, departmentCode, departmentDescription,
      toExcluded, axe) =>
        (idProduct, materialDescription,if (materialCreateTMSTP == "") "" else formatClassic.parseDateTime(materialCreateTMSTP), brandCode, brandDescription, subBrandCode,
          subBrandDescription, marketCode, marketDescription, targetCode, targetDescription, categoryCode, categoryDescription, rangeCode, rangeDescription, natureCode,
          natureDescription, departmentCode, departmentDescription, toExcluded, axe)
    }

    //lastSalesLines = RDD[NbRow (String), (idProduct (String), idCustpmer (String), lastPurchaseDate (Datetime), lastPurchaseDate (Int))]
    val lastSalesLines = lastSalesRDD.map(res => (Bytes.toString(res.getRow), res.raw.map(kv => Bytes.toString(kv.getValue)) match {
      case Array(idProduct, idCustomer, lastPurchaseDate) => (idProduct, idCustomer, new DateTime(lastPurchaseDate.toLong * 1000), lastPurchaseDate.toInt)
    } ))

    //rbLines = RDD[(idRollingBall (String), idCustomer (String))]
    val rbLines = rbRDD.filter(!_.contains("rb_client")).map(_.split(",")).map{
      case Array(idRB,idCustomer) => (idRB,idCustomer)
    }

    //tables utilisées plusieurs fois

    //caNbCaUnit : RDD[(String, Double, Int, Double)]
    //Etape 1 : on prend les product lines où la date est entre "today" et "today - 3 ans"
    //Etape 2 : on les groupent par idProduct
    //On obtient RDD[(idProduct, List[month, CA, Quantity)])]
    //Etape 3 : On le transforme en un RDD[(idProduct, sum(ca), sum(quantity), sum(ca)/sum(quantity))]
    val caNbCaUnit = monthResultProductLines
      .filter(x => x._2.isBefore(today) && x._2.isAfter(today.minusYears(3)))
      .groupBy(_._1)
      .map(x => (x._1,
                x._2.map(x => x._3).sum,
                x._2.map(_._4).sum,
                x._2.map(x => x._3).sum/x._2.map(_._4).sum)
      )

    //fonctions utiles



    /**
     * Calcul la contenance. Prend la partie avec des chiffres de la string fournie.
     * Donne le résultat en millilitre (ie si "2L", renvoit 2000)
     * @param param la string avec des chiffres
     * @return le chifre en millilitre
     */
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

    /**
     * Transforme le RDD caNbCaUnit en un RDD[(idProduct (String), indiceDuQuotient (int))]
     * Le quotient en question est le dernier élément du tuple dans caNbCaUnit, ie sum(ca)/sum(quantity)
     * La liste dont on récupère l'indice est List(0, 10, 25, 50, 80, 150)
     * @return Un RDD[(idProduct (String), indiceDuQuotient (int))]
     */
    def calculNotePrixAbsolu = {


      val catPrixAbs = List(0, 10, 25, 50, 80, 150)
      //NotePrixAbsolu = RDD[(String, Any)]
      //Etape 1 : on prend caNbCaUnit
      //Etape 2 : on transforme en RDD[(idProduct, <something>)]
      //Etape 2 a : <something> est categorization(sum(ca)/sum(quantity), catPrixAbs (list de valeurs))
      //On a ainsi un RDD[(idProduct, indiceDUQuotient)]
      //Etape 4 : on fait un map sur ce RDD
      //Etape 5 : On crée le même RDD, juste que si l'indice du quotient est 0 on le transforme en null
      //On a donc un RDD[(idProduct, indiceDuQuotient)]
      val notePrixAbsolu  = caNbCaUnit
        .map(x => (x._1, categorization(x._4, catPrixAbs)))
        .map(x => (x._1, if (x._2 == 0) null else x._2))
      notePrixAbsolu
    }


    /**
     * Permet de récupérer le RDD des prix relatif
     * Le RDD est au format RDD[(idProduct, categorization(sum(ca)/sum(quantity), List(0.64*prixRef, 0.8*prixRef, 1.25*prixRef, 1.56*prixRef), start = 1))]
     * @return le RDD des prix relatif, au format RDD[(idProduct, categorization(sum(ca)/sum(quantity), List(0.64*prixRef, 0.8*prixRef, 1.25*prixRef, 1.56*prixRef), start = 1))]
     */
    def calculNotePrixRelatif() = {

      /**
       *Suivant les différents paramètre, renvoit un String contenant la catégorie
       * @param axe L'axe du produit
       * @param rangeCode Le range code du produit
       * @param natureCode Le nature code du produit
       * @param materialDescription La material description du produit
       * @param categorieContenance L'indice donné par la fonction categorization(calcContenance(x._2), catContenance)
       * @param targetCode Le target code du produit
       * @return Un string indiquant la catégorie du produit
       */
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



      //RDD[(idProduct, categorieProd)]
      val categoriePrixRelatif = productLines
        .map(x => (x._1, catPrixRelatif(x._21, x._14, x._16, x._2, categorization(calcContenance(x._2), catContenance), x._10)))


      //Etape 1 : on prend RDD[(idProduct, categorieProd)]
      //Etape 2 : on fait un leftOuterJoin avec RDD[(idProduct, (sum(ca), sum(quantity), sum(ca)/sum(quantity)))]
      //On obtient donc un RDD[(idProduct, (categorieProd, (sum(ca), sum(quantity), sum(ca)/sum(quantity)))]
      //Etape 3 : on filter sur sum(quantity) >= 50 && categorieProd != "" && categorieProd != null
      //On obtient donc un RDD[(idProduct, (categorieProd, (sum(ca), sum(quantity), sum(ca)/sum(quantity)))]
      val categorieEtCA = categoriePrixRelatif
        .leftOuterJoin(caNbCaUnit.map(x => (x._1, (x._2, x._3, x._4))))
        .filter(x => x._2._2.getOrElse((null,0,null))._2 >= 50 & x._2._1 != "" & x._2._1 != null)


      //Etape 1 : on a RDD[(idProduct, (categorieProd, (sum(ca), sum(quantity), sum(ca)/sum(quantity)))]
      //Etape 2 : on le transforme en RDD[(categorieProd, sum(ca)/sum(quantity))]
      //Etape 3 : on group by key
      //Etape 4 : on le transforme en un RDD[(categorieProd, Array(sum(ca)/sum(quantity)))]
      //L'array est sorted
      //Etape 5 : on le transforme en RDD[(categorieProd, (Array[indice à 20%] + Array[indice à 80%])/2)]
      val prixRef = categorieEtCA
        .map(x => (x._2._1, x._2._2.get._3))
        .groupByKey()
        .map(x => (x._1,x._2.toArray.sortWith(_<_)))
        .map(x => (x._1, (x._2((x._2.length * 0.2).toInt)+ x._2((x._2.length*0.8).toInt))/2))


      //Etape 1 : on a RDD[(idProduct, (categorieProd, (sum(ca), sum(quantity), sum(ca)/sum(quantity)))]
      //Etape 2 : on le transforme en RDD[(categorieProd, (idProduct, sum(ca)/sum(quantity)))]
      //Etape 3 : on fait un leftOuterJoin avec prixRef
      //On obtient donc RDD[(categorieProd, ((idProduct, sum(ca)/sum(quantity)), prixRef))]
      //Etape 4 : on filtre en ne gardant que ceux avec prixRef != 0.0
      //Etape 5 : on le transforme en RDD[(idProduct, categorization(sum(ca)/sum(quantity), List(0.64*prixRef, 0.8*prixRef, 1.25*prixRef, 1.56*prixRef), start = 1))]
      val notePrixRelatif = categorieEtCA
        .map(x => (x._2._1, (x._1, x._2._2.get._3)))
        .leftOuterJoin(prixRef).filter(_._2._2.get != 0.0)
        .map(x => (x._2._1._1,categorization(x._2._1._2, List(0.64 * x._2._2.get, 0.8 * x._2._2.get, 1.25 * x._2._2.get, 1.56 * x._2._2.get),1)))


      notePrixRelatif
    }


    // Notes parfums alcools
    //Calcul des notes Gestes
    //Etape 1 : on filtre sur axe == "PARFUM" && rangeCode == "ALCOOLS"
    //Etape 2 : on trasforme en RDD[(idProduct, Array(flag_EFP, flag_EDT, flag_EDC, flag_Extraits, flag_vapo_sac, flag_vapo, flag_flacon, flag_sray,  categorization(calcContenance(materialDescription), catContenance,1)
    //On a donc un RDD[(idProduct, Array(flags et indice))]
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
    //Etape 1 : on filtre sur axe == "PARFUM"
    //On tranforme en RDD[(idProduct, 1 ou 0 suivant le match d'un parfum)]
    val notesParfum = productLines.filter(_._21 == "PARFUM").map(x => (x._1,
      (x._5, x._7) match {
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
    //On transforme en RDD[(idProduct, Array(
    // flag_selectif,
    // flag_diffuson
    // flag_sepho
    // flag_exclusivite
    // flag_edition_limitee
    //flag_coffret
    // flag_mixte
    // flag_homme
    // flag_femme
    // flag_enfant
    //flag recharge))]
    val notesCrossUnivers = productLines
      .map(x => (x._1, Array(
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


    //Etape 1 : On transforme en RDD[(idProduct, (brandCode, marketcode)))]
    //Etape 2 : On fait un leftOuterJoin avec RDD[(idProduct, sum(ca)/sum(quantity))]
    //Etape 3 : On transforme en RDD[(idProduct, indice par rapport à (brandCode, marketCode) et une liste diffrénte par couple
    //Etape 4 : On transforme en RDD[(idProduct, indice (null si l'indice était 0))]
    val noteRareteSKU = productLines
      .map(x => (x._1, (x._4, x._8))).leftOuterJoin(caNbCaUnit.map(x => (x._1, x._3)))
      .map(x => (x._1,
      (x._2._1._1,x._2._1._2) match {
        case ("SELECTIF",_) => categorization(x._2._2, List(0,12,60,290,1800,5900))
        case ("DIFFUSION",_) => 6
        case (_,"SEPHO") => categorization(x._2._2, List(0,120,600,2900,18000,59000))
        case ("EMERGENT",_) => categorization(x._2._2, List(0,100,540,2610,16200,53100))
        case _ => null}))
      .map(x => (x._1, if (x._2 ==0) null else x._2))


    //On filtre sur axe == "MAQUILLAGE"
    //On transforme en RDD[(idProduct, Array(
    //flag_yeux_coffret_eyeliner_fardapaup_visage_anticerne
    //flag_visage_anticerne_solaires_maquillage
    //flag_ongles_coffret_vao
    //flag_levres_coffrets_ral)]
    val notesMaquillage = productLines.filter(_._21 == "MAQUILLAGE").map( x => (x._1, Array(
      if (x._14 == "YEUX" || (x._14 == "COFFRETS" && (x._16 == "EYELINER" || x._16 == "FARDAPAUP")) || (x._14 == "VISAGE" && x._16 == "ANTICERNE")) 1 else 0,
      if ((x._14 == "VISAGE" && x._16 != "ANTICERNE") || (x._14 == "SOLAIRES" && x._16 == "MAQUILLAGE") ) 1 else 0,
      if (x._14 == "ONGLES" || (x._14 == "COFFRETS" && x._16 == "VAO") ) 1 else 0,
      if (x._14 == "LEVRES" || (x._14 == "COFFRETS" && x._16 == "RAL") ) 1 else 0
      )
      )
    )


    //On filtre sur axe == "SOIN"
    //On transforme en RDD[(idProduct, Array(
    //flag_Zone d'application corps
    //flag_Zone d'application visage
    //flag_Zone d'application ongles
    //flag_Zone d'application capillaire
    //flag_Besoin/Fonction Hygiène
    //flag_Besoin/Fonction Capillaire
    //flag_Besoin/Fonction Solaire
    //flag_Besoin/Fonction Nettoyage
    //flag_Besoin/Fonction Minceur
    //flag_Besoin/Fonction Ongles
    //flag_Besoin/Fonction Hydratation
    //flag_Besoin/Fonction Anti-âge
    //flag_Besoin/Fonction Complément Alimentaire
    //flag_Besoin/Fonction Démaquillant
    //flag_Besoin/Fonction Eau de soin
    //flag_Besoin/Fonction Masque/Gommage
    //flag_Besoin/Fonction Soin Quotidien
    //flag_Besoin/Fonction Traitement Spécifique
    //flag_Besoin/Fonction Marque Emergente
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


    /**
     * Calcule les notes subjectives
     * @return Un RDD[(idProduct, score)]
     */
    def calculNotesSubjectives = {


      //Etape 1 : on filtre productLines sur
      // idProduct != "63301" && brandcode != "ECHAN" && brandCode != "PLV" && breandDescription != "FIDELITE" && subBrandCode != "SEPHO60"
      // && rangeCode != "BAIN-BE"
      //On transforme en RDD[(idProduct, true)]
      val product4Timer = productLines.filter(x => x._1 != "63301" && x._4 != "ECHAN" && x._4 != "PLV" && x._5 != "FIDELITE" && x._6 != "SEPHO60" && x._14 != "BAIN B-E")
        .map(x => (x._1, true))

      //Etape 1 : On recupere les lignes de ventes
      //Etape 2 : on filtre sur :
      // nbTickets > 4 &&
      // Sum( Sum (quantity * price) ) > 0 &&
      // nb_distinct_id_product_from_ticket > 4 &&
      // idCardStatus in [2, 3, 4]
      //Etape 3 : on transforme en RDD[(idClient, true)]
      val clients4Timer = extractPurchaseLines()
        .filter(
          x => x._6.length > 4 &&
          x._6.map(_._3.map(y => y._2 * y._3).sum).sum > 0.0 &&
          Tools.concatener(x._6.map(_._3.map(_._1))).distinct.length > 4 &&
          (x._4 == 2 || x._4 == 3 || x._4 == 4)
        )
        .map(x => (x._1, true))

      //ATTENTION!!!!!!!!!!!!!!!!!!!!!!!!!!!!! premier filter temporaire pour éviter Java Heap Space

      //Etape 1 : on a lastSalesLines = RDD[NbRow (String), (idProduct (String), idCustpmer (String), lastPurchaseDate (Datetime), lastPurchaseDate (Int))]
      //Etape 2 : on filtre sur lastPurchaseDate.getMonthOfYear > 11
      //Etape 3 : on transforme en RDD[(idProduct, idCustomer)]
      //Etape 4 : on leftOuterJoin avec product4Timer (RDD[(idProduct, true)])
      //Etape 5 : on filtre sur true
      //Etape 6 : on transforme en RDD[(idCustomer, idProduct)]
      //Etape 7 : on leftOuterJoin avec clients4Timer (RDD[(idClient, true)])
      //Etape 8 : on filtre sur true
      //Etape 9 : on leftOuterJoin avec rbLines traonsformé (RDD[(idCustomer (String), idRollingBall (String))])
      //Etape 10 : on transforme en RDD[(idCustomer, idProduct, idRollingBall)]
      //On a donc un RDD[(idCustomer, idProduct, idRollingBall)]
      val lastSales4TimerLines = lastSalesLines
        .filter(_._2._3.getMonthOfYear > 11)
        .map(x => (x._2._1, x._2._2))
        .leftOuterJoin(product4Timer)
        .filter(_._2._2.getOrElse(false))
        .map(x => (x._2._1 , x._1))
        .leftOuterJoin(clients4Timer)
        .filter(_._2._2.getOrElse(false))
        .map(x => (x._1, x._2._1))
        .leftOuterJoin(rbLines.map(x => (x._2,x._1)))
        .map(x => (x._1, x._2._1, x._2._2.getOrElse(null)))



      val nb_client_tot = lastSales4TimerLines.map(_._1).distinct.count
      val nb_client_rb = rbLines.countByKey

      println("-----------------------------------------------------")
      println(nb_client_tot)
      println("-----------------------------------------------------")

      //Etape 1 : On a RDD[(idCustomer, idProduct, idRollingBall)]
      //Etape 2 : on transforme en RDD[(idProduct, idRollingBall)]
      //Etape 3 : On group by idProduct
      //On a donc un RDD[(idProduct, Seq[(idProduct, idRollingBall)])]
      //Etape 4 : On transforme en RDD[(idProduct, <something>)]
      //Etape 4 a : Le something est :
      //Etape 4 b : On prends la seq, on la transforme en Array[(idProduct, idRollingBall)]
      //Etape 4 c : On group by idRollingBall
      //Etape 4 c : On a donc un Map[(idRollingBall, Array[(idProduct, idRollingBall)])]
      //Etape 4 d : On filtre sur idRollingBall dans nb_client_rb
      //Etape 4 e : On transforme en (nb_(idProduct_idRolling_ball_filtre)/nb_client_pour_idRollingBall) / (nb_id_product_id_rollingball_non_filtre/nb_client_tot)
      //On a donc un RDD[(idProduct, score)]
      val notesSubjectives = lastSales4TimerLines
        .map(x => (x._2, x._3))
        .groupBy(_._1)
        .map(x => {
            (x._1,
              x._2.toArray
                .groupBy(_._2)
                .filter(z => nb_client_rb.contains(z._1))
                .map(y => {
                  if(nb_client_rb.get(y._1).getOrElse(0) == 0 || x._2.length == 0 || nb_client_tot == 0){
                    null
                  }else{
                    (y._2.length.toDouble / nb_client_rb.get(y._1).get.toDouble) / (x._2.length.toDouble / nb_client_tot.toDouble)
                  }
                 }
                )
            )
        })

      notesSubjectives


    }



    ///////////////////Debut calcul agregat/////////////////////////
    /**
     * Donne l'agregat par produit
     * @return Un RDD[(idProduct, axe, agregat_id)]
     */
    def get_agregat_produit()={

      def get_agregat_id_maquillage(sub_brand_code: String, range_code: String, nature_code: String, target_code: String, material_description: String):String={
        val first_part_id = sub_brand_code + range_code + nature_code + target_code

        val split_desc = material_description.split(" ")

        if(split_desc.length > 1){
          val premier = split_desc(0).substring(0, math.min(split_desc(0).size, 5))
          val second = split_desc(1).substring(0, math.min(split_desc(1).size, 5))
          return first_part_id + premier + second
        }else{
          val premier = split_desc(0).substring(0, math.min(split_desc(0).size, 5))
          return first_part_id + premier
        }
      }

      def get_agregat_id_parfum(sub_brand_code: String, range_code: String, nature_code: String, target_code: String): String ={
        sub_brand_code + range_code + nature_code + target_code
      }

      def get_agregat_id_soin(sub_brand_code: String, material_description: String): String ={
        val new_m_desc = remove_contenance_ml_material_description(material_description)
        sub_brand_code + new_m_desc
      }


      //Etape 1 : on a RDD[(idProduct, 20 param)]
      //Etape 2 : on transforme en RDD[(idProduct, axe, sub_brand_code, range_code, nature_code, target_code, material_description)]
      //Etape 3 : on remplace certains mots dans material_description
      //Etape 4 : on transforme en RDD[(idProduct, axe, agregat_id)]
      val p = productLines
        .map(t => (t._1, t._21, t._6, t._14, t._16, t._10, t._2))
        .map(t => {
            var m_desc = t._7.replace("RAL", "ROUGE A")
            m_desc = m_desc.replace("FDT", "FOND DE")
            m_desc = m_desc.replace("MASCARA", "MASC")
            (t._1, t._2, t._3, t._4, t._5, t._6, m_desc)
          })
        .map(t =>{
            val agregat_id = if(t._2 == "PARFUM"){
              get_agregat_id_parfum(t._3, t._4, t._5, t._6)
            }else if(t._2 == "MAQUILLAGE"){
              get_agregat_id_maquillage(t._3, t._4, t._5, t._6, t._7)
            }else if(t._2 == "SOIN"){
              get_agregat_id_soin(t._3, t._7)
            }else{
              ""
            }
        (t._1, t._2, agregat_id)
        })

      p
    }
    ///////////////////Fin calcul agregat///////////////////////////
    ///////////////////Debut Rare Ligne/////////////////////////////
    def calcul_type_repandu_ligne(axe: String, quantity: Int): Int ={
      val seuils_maquillage = List(50, 200, 600, 2400, 7500)
      val seuils_parfum = List(70, 300, 1800, 2000, 80000)
      val seuils_soin = List(90, 350, 1200, 6500, 25000)

      if(axe == "MAQUILLAGE"){
        categorization(quantity, seuils_maquillage) + 1
      }else if(axe == "PARFUM"){
        categorization(quantity, seuils_parfum) + 1
      }else if(axe == "SOIN"){
        categorization(quantity, seuils_soin) + 1
      }else{
        -1
      }
    }
    /**
     * Calcul la note de rarete de la ligne
     * @return Un RDD[(idProduct, score_repandu_ligne)]
     */
    def get_note_rarete_ligne()={
      //Etape 1 : on a RDD[(idClient, idGender, age, idCardStatus, idSegmentRFM, List[(idTicket, pruchaseDate, List[(idProduct, quantity, price)])])
      //Etape 2 : on transforme en RDD[(idTicket, purchaseDate, List[(idProduct, quantity, price)])]
      //Etape 3 : on transforme en RDD[(idProduct, quantity, price)]
      //Etape 4 : on transforme en RDD[(idProduct, quantity)]
      //Etape 5 : on groupBy idProduct
      //Etape 6 : on transforme en RDD[(idProduct, tot_quantity)]
      val idProduct_quantity = extractPurchaseLines()
        .flatMap(t => t._6)
        .flatMap(t => t._3)
        .map(t => (t._1, t._2))
        .groupBy(_._1)
        .map(t => (t._1, t._2.map(_._2).sum))

      //Etape 1 : on a RDD[(idProduct, axe, agregat_id)]
      //Etape 2 : on transforme en RDD[(idProduct, (axe, agregat_id)]
      val idProduct_axe_agregat = get_agregat_produit()
        .map(t => (t._1, (t._2, t._3)))

      //Etape 1 : on a RDD[(idProduct, tot_quantity)]
      //Etape 2 : on leftOuterJoin avec idProduct_axe_agregat_id
      //Etape 3 : on transforme en RDD[(idProduct, tot_quantity, axe, agregat_id)]
      //Etape 4 : on filtre sur axe et agregat_id != ""
      //Etape 5 : on groupBy agregat_id
      //Etape 6 : on transforme en RDD[(agregat_id, axe, tot_quantity, Seq[idProduct])]
      val agregatId_axe_quantity_seqIdProduct = idProduct_quantity
        .leftOuterJoin(idProduct_axe_agregat)
        .map(t => (t._1, t._2._1, t._2._2.getOrElse(("", ""))._1, t._2._2.getOrElse(("", ""))._2))
        .filter(t => !(t._3 == "" || t._4 == ""))
        .groupBy(_._4)
        .map(t => (t._1, t._2(0)._3, t._2.map(_._2).sum, t._2.map(_._1)))

      //Etape 1 : on a RDD[(agregat_id, axe, tot_quantity, Seq[idProduct])]
      //Etape 2 : on transforme en RDD[(agregat_id, score, distinct Seq[IdProduct])]
      //Etape 3 : on transforme en RDD[(idProduct, score_repandu_ligne)]
      val idProduct_score = agregatId_axe_quantity_seqIdProduct
        .map(t => (t._1, calcul_type_repandu_ligne(t._2, t._3), t._4.distinct))
        .flatMap(t => t._3.map(p => (p, t._2)))

      idProduct_score
    }
    ///////////////////Fin Rare Ligne///////////////////////////////

    ///////////////////Debut type parfum////////////////////////////
    /**
     * Calcul les typologies des parfums
     * @return Un RDD[(idProduct, Array(is_classique, is_icone, is_espoir, is_revelation))]
     */
    def get_note_type_parfum()={
      //Pour pouvoir faire le minBy(DateTime)
      implicit val orderDate : Ordering[DateTime] = Ordering.by(t => t.getMillis)

      //Etape 1 : on a RDD[(idClient, idGender, age, idCardStatus, idSegmentRFM, List[(idTicket, pruchaseDate, List[(idProduct, quantity, price)])])
      //Etape 2 : on transforme en RDD[(idTicket, purchaseDate, List[(idProduct, quantity, price)])]
      //Etape 3 : on filtre sur purchaseDate dans [D -1; D[
      //Etape 4 : on transforme en RDD[(idProduct, quantity, price)]
      //Etape 5 : on transforme en RDD[(idProduct, ca (== quantity * price)]
      //Etape 6 : on groupBy idProduct
      //Etape 7 : on transforme en RDD[(idProduct, ca_tot)]
      val idProduct_ca_ventes_fid = extractPurchaseLines()
        .flatMap(_._6)
        .filter(t => formatClassic.parseDateTime(t._2).isBefore(today) && formatClassic.parseDateTime(t._2).isAfter(today.minusYears(1)))
        .flatMap(_._3)
        .map(t => (t._1, t._2 * t._3))
        .groupBy(_._1)
        .map(t => (t._1, t._2.map(_._2).sum))

      //Etape 1 : on a RDD[(idProduct, month, CA, Quantity)]
      //Etape 2 : on filtre sur month dans [D - 1; D[
      //Etape 3 : on groupBy idProduct
      //Etape 4 : on transforme en RDD[(idProduct, ca_tot)]
      val idProduct_ca_ventes_non_fid = monthResultProductLines
        .filter(t => t._2.isBefore(today) && t._2.isAfter(today.minusYears(1)))
        .groupBy(_._1)
        .map(t => (t._1, t._2.map(_._3).sum))

      //Etape 1 : on a RDD[(idProduct, ca_tot)]
      //Etape 2 : on leftOuterJoin avec idProduct_ca_ventes_non_fid
      //Etape 3 : on transforme en RDD[(idProduct, ca_tot)]
      val idProduct_ca_tot = idProduct_ca_ventes_fid
        .leftOuterJoin(idProduct_ca_ventes_non_fid)
        .map(t => (t._1, t._2._1 + t._2._2.getOrElse(0d)))


      //Creation de RDD[(idProduct, firstDateAchat)]
      //Etape 1 : on a RDD[(idProduct, month, CA, Quantity)]
      //Etape 2 : on transforme en RDD[(idProduct, month)]
      //Etape 3 : on groupBy idProduct
      //Etape 4 : on transforme en RDD[(idProduct, min(idProduct, month))] (le min est fait sur la date)
      //Etape 5 : on transforme en RDD[(idProduct, firstDateAchat)]
      val idProd_firstAchat = monthResultProductLines
        .map(tuple => (tuple._1, tuple._2))
        .groupBy(_._1)
        .map(t => (t._1, t._2.minBy(_._2)))
        .map(t => (t._1, t._2._2))


      //Etape 1 : on a RDD[(idProduct, 20 autres)]
      //Etape 2 : on transforme en RDD[(idProduct, sub_brand_code)]
      val idProduct_subBrandCode = productLines
        .map(t => (t._1, t._6))

      //Etape 1 : on a RDD[(idProduct, ca_tot)]
      //Etape 2 : on leftOuterJoin avec RDD[(idProduct, firstDateAchat)]
      //Etape 3 : on transforme en RDD[(idProduct, (ca_tot, first_achat)]
      //Etape 4 : on filtre sur first_achat != null
      //Etape 5 : on leftOuterJoin avec RDD[(idProduct, sub_brand_code)]
      //Etape 6 : on tranforme en RDD[(idProduct, ca_tot, first_achat, sub_brand_code)]
      //Etape 7 : on filtre sur sub_brand_code != null
      val idProduct_caTot_firstAchat_subBrandCode = idProduct_ca_tot
        .leftOuterJoin(idProd_firstAchat)
        .map(t => (t._1, (t._2._1, t._2._2.getOrElse(null))))
        .filter(_._2._2 != null)
        .leftOuterJoin(idProduct_subBrandCode)
        .map(t => (t._1, t._2._1._1, t._2._1._2, t._2._2.getOrElse(null)))
        .filter(_._4 != null)

      //Etape 1 : on a RDD[(idProduct, ca_tot, first_achat, sub_brand_code)]
      //Etape 2 : on groupBy sub_brand_code
      //Etape 3 : on transforme en RDD[(sub_brand_code, min_first_achat, ca_tot, seq[idProduct])]
      //Etape 4 : on transforme en RDD[(sub_brand_code, anciennete, ca_tot, seq[idProduct])]
      val subBrandCode_anciennete_caTot_seqIdProduct = idProduct_caTot_firstAchat_subBrandCode
        .groupBy(_._4)
        .map(t => (t._1, t._2.map(_._3).min, t._2.map(_._2).sum, t._2.map(_._1)))
        .map(t => {
          val nb_day = Days.daysBetween(today, t._2).getDays
          (t._1, nb_day.toDouble / 365d, t._3, t._4)
        })


      //Etape 1 : on a RDD[(sub_brand_code, anciennete, ca_tot, seq[idProduct])]
      //Etape 2 : on transforme en RDD[(sub_brand_code, is_classique, is_icone, is_espoir, is_revelation, seq[idProduct])]
      //Etape 3 : on transforme en RDD[(idProduct, Array(is_classique, is_icone, is_espoir, is_revelation))]
      val idProduct_typo_parfums = subBrandCode_anciennete_caTot_seqIdProduct
        .map(t => {
        val is_classique = if(t._3 > 550000 && t._3 < 2200000 && t._2 >= 4.5) 1 else 0
        val is_icone = if(t._3 >= 2200000 && t._2 >= 4.5) 1 else 0
        val is_espoir = if(t._3 > 550000 && t._3 < 2200000 && t._2 < 4.5) 1 else 0
        val is_revelation = if(t._3 >= 2200000 && t._2 < 4.5) 1 else 0
        (t._1, is_classique, is_icone, is_espoir, is_revelation, t._4)
      })
        .flatMap(t => t._6.map(p => (p, Array(t._2, t._3, t._4, t._5))))

      idProduct_typo_parfums
    }
    ///////////////////Fin type parfum//////////////////////////////

    //On fait des leftOuterJoin avec plein de notes, on obtient un RDD[(idProduct, Array(flag et autres)]
    val notesProduit = notesCrossUnivers
      .leftOuterJoin(notesSoin).map(x => (x._1, x._2._2.getOrElse(null) +: x._2._1))
      .leftOuterJoin(notesMaquillage).map(x => (x._1, x._2._2.getOrElse(null) +: x._2._1))
      .leftOuterJoin(noteRareteSKU).map(x => (x._1, x._2._2.getOrElse(null) +: x._2._1))
      .leftOuterJoin(notesParfum).map(x => (x._1, x._2._2.getOrElse(null) +: x._2._1))
      .leftOuterJoin(notesGestes).map(x => (x._1, x._2._2.getOrElse(null) +: x._2._1))
      .leftOuterJoin(calculNotePrixAbsolu).map(x => (x._1, x._2._2.getOrElse(null) +: x._2._1))
      .leftOuterJoin(calculNotePrixRelatif).map(x => (x._1, x._2._2.getOrElse(null) +: x._2._1))
      .leftOuterJoin(calculNotesSubjectives).map(x => (x._1, x._2._2.getOrElse(null) +: x._2._1))
      .leftOuterJoin(get_note_rarete_ligne).map(x => (x._1, x._2._2.getOrElse(null) +: x._2._1))
      .leftOuterJoin(get_note_type_parfum).map(x => (x._1, x._2._2.getOrElse(null) +: x._2._1))


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
    val monthResultProductRDD = hdfsFetcher.readData(monthResultProductTable)

    //monthResultProductLines = RDD[(idProduct (String), month (Datetime), CA (Double), Quantity (Int))]
    val monthResultProductLines = monthResultProductRDD.map(_.split("\001")).map{
      case Array(idProduct, month, ca, quantity) => (idProduct, formatClaraVista.parseDateTime(month), ca.toDouble, quantity.toInt)
    }

    /////////////////////Debut Note Nouveaute///////////////////////
    /**
     * Calcul la note nouveaute
     * @return Un RDD[(idClient, noteNouveaute)]
     */
    def getNotesNouveaute()={
      //Pour pouvoir faire le minBy(DateTime)
      implicit val orderDate : Ordering[DateTime] = Ordering.by(t => t.getMillis)


      //Creation de RDD[(idProduct, firstDateAchat)]
      //Etape 1 : on a RDD[(idProduct, month, CA, Quantity)]
      //Etape 2 : on transforme en RDD[(idProduct, month)]
      //Etape 3 : on groupBy idProduct
      //Etape 4 : on transforme en RDD[(idProduct, min(idProduct, month))] (le min est fait sur la date)
      //Etape 5 : on transforme en RDD[(idProduct, firstDateAchat)]
      val idProd_firstAchat = monthResultProductLines
        .map(tuple => (tuple._1, tuple._2))
        .groupBy(_._1)
        .map(t => (t._1, t._2.minBy(_._2)))
        .map(t => (t._1, t._2._2))


      //Etape 1 : on a RDD[(idClient, idGender, age, idCardStatus, idSegmentRFM, List[(idTicket, pruchaseDate, List[(idProduct, quantity, price)])]))]
      //Etape 2 : on transforme en RDD[(idClient, List[(purchaseDate, List[(idProduct, quantity, price)]
      //Etape 3 : on transforme en RDD[[idClient, List[(purchaseDate, List[idProduct]]]
      //Etape 4 : on transforme en RDD[(idClient, List[(purchaseDate, idProduct)]]
      //Etape 5 : on transforme en RDD[(idProduct, purchaseDate, idClient)]
      //(on change l'ordre pour pouvoir faire le leftJoin ensuite, et on transforme la date en DateTime au lieu de String)
      //Etape 6 : on transforme en RDD[(idProduct, (idProduct, purchaseDate, idClient)] (pour pouvoir leftJoin ensuite)
      val idProd_dateAchat_idClient = extractPurchaseLines()
        .map(t => (t._1, t._6.map(tickets => (tickets._1, tickets._3))))
        .map(t => (t._1, t._2.map(tickets => (tickets._1, tickets._2.map(prods => prods._1)))))
        .map(t => (t._1, t._2.flatMap(list_list => list_list._2.map(idProd => (list_list._1, idProd)))))
        .flatMap(t => t._2.map(date_idProd => (date_idProd._2, formatClaraVista.parseDateTime(date_idProd._1), t._1)))
        .keyBy(t => t._1)


      //Etape 1 : on a idProd_dateAchat_idClient
      //Etape 2 : on outerLeftJoin avec idProd_firstAchat, on obtient RDD[(idProduct, ((idProduct, purchaseDate, idClient), Option(firstAchat, CA)))]
      //Etape 3 : on transforme en RDD[(idProduct, purchaseDate, idClient, firstAchat)]
      //Etape 4 : on enleve toutes les lignes sans dateTime pour firstAchat
      //Etape 5 : on transforme en RDD[(idProduct, (purchaseDate, idClient, firstAchat, typeAnciennete))]
      val idProd_dateAchat_idClient_firstAchat_typeAnciennete = idProd_dateAchat_idClient
        .leftOuterJoin(idProd_firstAchat)
        .map(t => (t._1, t._2._1._2, t._2._1._3, t._2._2.getOrElse(null)))
        .filter(t => t._4 != null)
        .map(t => {
        val list_date_anciennete = List(3, 6, 12, 24)
        val diffDate = Months.monthsBetween(t._2, t._4).getMonths
        val typeAnciennete = categorization(diffDate, list_date_anciennete) + 1 //categorization renvoit l'indice, on veut indice + 1
        (t._1, (t._2, t._3, t._4, typeAnciennete))
        })

      //Etape 1 : on récupère le RDD des lignes de vente (id, idGender, age, idCardStatus, idSegmentRFM, List[(idTicket, pruchaseDate, List[(idProduct, quantity, price)])])
      //Etape 2 : on transformer en RDD[(idClient, <something>)]
      //Etape 3 : le deuxième élément est Tools.concatener(<something>)
      //Etape 4 : le something est List[List[idProduct, quantity*price]]
      //Etape 5 : on a donc un RDD[(idClient, List[(idProduct, quantity*price)])]
      //Etape 6 : on transforme en RDD[((idClient, idProduct), CA)]
      val client_list_prod = extractPurchaseLines()
        .map(x => {
        (x._1,Tools.concatener(x._6.map(_._3.map(y => (y._1, y._2 * y._3)))).asInstanceOf[List[(String, Float)]])
        })
        .flatMap(t => t._2.map(prod => ((t._1, prod._1), prod._2)))

      //Etape 1 : on a RDD[(idProduct, (purchaseDate, idClient, firstAchat, typeAnciennete))]
      //Etape 2 : on transforme en RDD[((idClient, idProd), typeAnciennete
      //Etape 3 : on leftOuterJoin avec client_list_prod
      //Etape 4 : on transforme en RDD[(idClient, CA, typeAnciennete)]
      //Etape 5 : on filtre sur les CA non egaux à -1f
      //On a donc RDD[(idClient, CA, typeAnciennete)]
      val idClient_ca_typeAnciennete = idProd_dateAchat_idClient_firstAchat_typeAnciennete
          .map(t => ((t._2._2, t._1), t._2._4))
          .leftOuterJoin(client_list_prod)
          .map(t => (t._1._1, t._2._2.getOrElse(-1f), t._2._1))
          .filter(t => t._2 != -1f)


      //Etape 1 : on a RDD[(idClient, CA, typeAnciennete)]
      //Etape 2 : on transforme en RDD[(idClient, CA, typeAnciennete)]
      //Etape 3 : on groupBy idClient, on a RDD[(idClient, Seq[(idClient, CA, typeAnciennete)]
      //Etape 4 : on transforme en RDD[(idClient, Seq[(CA, anciennete*CA)]
      //Etape 5 : on transforme en RDD[(idClient, CA_tot, sum(anciennete*CA))]
      //Etape 6 : on transforme en RDD[(idClient, noteNouveaute)]
      val notes_nouveautes = idClient_ca_typeAnciennete
        .groupBy(_._1)
        .map(t => (t._1, t._2.map(prods => (prods._2, prods._3 * prods._2))))
        .map(t => (t._1, t._2.map(p => p._1).sum, t._2.map(p => p._2).sum))
        .map(t => (t._1, t._3/t._2))

      notes_nouveautes
    }
    ////////////////////Fin notes nouveaute/////////////////////////

    ////////////////////Debut note cross gender/////////////////////
    /**
     * Calcul la note crossgender
     * @return Un RDD[(idClient, note_crossgender)]
     */
    def crossGender()={

      val target_code_homme = "HOMME"
      val target_code_femme = "FEMME"
      val idGender_homme = 1
      val idGender_femme = 2

      //Etape 1 : on récupère le RDD des lignes de vente (id, idGender, age, idCardStatus, idSegmentRFM, List[(idTicket, pruchaseDate, List[(idProduct, quantity, price)])])
      //Etape 2 : on transformer en RDD[(idClient, idGender <something>)]
      //Etape 3 : le deuxième élément est Tools.concatener(<something>)
      //Etape 4 : le something est List[List[idProduct, quantity*price]]
      //Etape 5 : on a donc un RDD[(idClient, idGender, List[(idProduct, quantity*price)])]
      //Etape 6 : on transforme en RDD[(idProduct, (idClient, idGender, CA))]
      val idProd_idClient_idGender_CA = extractPurchaseLines()
        .map(x => {
        (x._1, x._2, Tools.concatener(x._6.map(_._3.map(y => (y._1, y._2 * y._3)))).asInstanceOf[List[(String, Float)]])
        })
        .flatMap(t => t._3.map(prod => (prod._1, (t._1, t._2, prod._2))))


      val productRDD = hdfsFetcher.readData(productTable)

      //Etape 1 : on a RDD[(idProduct, divers flag et infos)]
      //Etape 2 : on transforme en RDD[(idProduct, targetCode)]
      val idProduct_targetCode = productRDD.map(_.split("\001")).map{
        case Array(idProduct, materialDescription, materialCreateTMSTP, brandCode, brandDescription, subBrandCode, subBrandDescription, marketCode, marketDescription,
        targetCode, targetDescription, categoryCode, categoryDescription, rangeCode, rangeDescription, natureCode, natureDescription, departmentCode, departmentDescription,
        toExcluded, axe) =>
          (idProduct, materialDescription,if (materialCreateTMSTP == "") "" else formatClassic.parseDateTime(materialCreateTMSTP), brandCode, brandDescription, subBrandCode,
            subBrandDescription, marketCode, marketDescription, targetCode, targetDescription, categoryCode, categoryDescription, rangeCode, rangeDescription, natureCode,
            natureDescription, departmentCode, departmentDescription, toExcluded, axe)
      }.map(t => (t._1, t._10))


      //Etape 1 : on a RDD[(idProduct, (idClient, idGender, CA))]
      //Etape 2 : on leftOuterJoin avec idProduct_targetCode
      //Etape 3 : on transforme en RDD[(idClient, idGender, targetCode, CA)]
      //Etape 4 : on filtre sur les targetCode valide
      //Etape 5 : on transforme en RDD[(idClient, crossgender, CA)]
      val idClient_crossgender_ca = idProd_idClient_idGender_CA
        .leftOuterJoin(idProduct_targetCode)
        .map(t => (t._2._1._1, t._2._1._2, t._2._2.getOrElse("error"), t._2._1._3))
        .filter(t => t._3 != "error")
        .map(t => {
        val idGender = t._2
        val targetCode = t._3
        val cross = if(
          (idGender == idGender_homme && targetCode == target_code_femme) ||
          (idGender == idGender_femme && targetCode == target_code_homme)
          ) 1
          else 0
        (t._1, cross, t._4)
      })


      //Etape 1 : on a RDD[(idClient, crossgender, CA)]
      //Etape 2 : on groupBy idClient
      //On a donc RDD[(idClient, Seq[(idClient, crossgender, CA)])]
      //Etape 3 : on transforme en RDD[(idClient, Seq[(crossgender*CA)], CA_tot)]
      //Etape 4 : on transforme en RDD[(idClient, note_crossgender)]
      val note_cross_gender = idClient_crossgender_ca
        .groupBy(_._1)
        .map(t => (t._1, t._2.map(prod => prod._2 * prod._3), t._2.map(_._3).sum))
        .map(t => (t._1, t._2.sum/t._3))

      note_cross_gender
    }
    ////////////////////Fin note cross gender///////////////////////

    ////////////////////Debut familles olfactives///////////////////
    /**
     * Calcul les notes olfactives
     * @return Un RDD[(idClient, (score_AROMATIQUE, score_BOISE, score_CHYPRE, score_FLORAL, score_FOUGERE, score_HESPERIDE, score_ORIENTAL))]
     */
    def get_notes_familles_olfactives()={

      //Etape 1 : RDD[(material_description, idProduct, sub_brand_code, famille, sousfamille)]
      //Etape 2 : on transforme en RDD[(idProduct, famille)]
      val idProduct_familles_olfactives = csvFetcher.readData("/home/spark/workspace/IdeaProject/clarabox/data/Sephora/Familles_olfactives.csv", has_header = true, delimiter = ",", string_delimiter = "\"")
        .map(t => (t(1), t(3)))


      val productRDD = hdfsFetcher.readData(productTable)

      //Etape 1 : on a RDD[(idProduct, divers flag et infos)]
      //Etape 2 : on transforme en RDD[(idProduct, axe)]
      val idProduct_axe = productRDD.map(_.split("\001")).map{
        case Array(idProduct, materialDescription, materialCreateTMSTP, brandCode, brandDescription, subBrandCode, subBrandDescription, marketCode, marketDescription,
        targetCode, targetDescription, categoryCode, categoryDescription, rangeCode, rangeDescription, natureCode, natureDescription, departmentCode, departmentDescription,
        toExcluded, axe) =>
          (idProduct, materialDescription,if (materialCreateTMSTP == "") "" else formatClassic.parseDateTime(materialCreateTMSTP), brandCode, brandDescription, subBrandCode,
            subBrandDescription, marketCode, marketDescription, targetCode, targetDescription, categoryCode, categoryDescription, rangeCode, rangeDescription, natureCode,
            natureDescription, departmentCode, departmentDescription, toExcluded, axe)
      }.map(t => (t._1, t._21))



      //Etape 1 : on récupère le RDD des lignes de vente (id, idGender, age, idCardStatus, idSegmentRFM, List[(idTicket, pruchaseDate, List[(idProduct, quantity, price)])])
      //Etape 2 : on transformer en RDD[(idClient, idGender <something>)]
      //Etape 3 : le deuxième élément est Tools.concatener(<something>)
      //Etape 4 : le something est List[List[idProduct, quantity*price]]
      //Etape 5 : on a donc un RDD[(idClient, List[(idProduct, quantity*price, quantity)])]
      //Etape 6 : on transforme en RDD[(idProduct, (idClient, ca, quantity))]
      val idProduct_idClient_CA_quantity = extractPurchaseLines()
        .map(x => {
        (x._1, Tools.concatener(x._6.map(_._3.map(y => (y._1, y._2 * y._3, y._2)))).asInstanceOf[List[(String, Float, Int)]])
      }).flatMap(t => t._2.map(prod => (prod._1, (t._1, prod._2, prod._3))))


      //Etape 1 : on a RDD[(idProduct, axe)]
      //Etape 2 : on garde les produits avec axe == "PARFUM"
      val idProduct_axeParfum = idProduct_axe
        .filter(_._2 == "PARFUM")

      //Etape 1 : on a RDD[(idProduct, axe)]
      //Etape 2 : on leftOuterJoin avec famille_olfactives
      //Etape 3 : on transforme en RDD[(idProduct, (famille, axe))]
      //Etape 4 : on filtre sur famille != ""
      val idProduct_famille_axe = idProduct_axeParfum
        .leftOuterJoin(idProduct_familles_olfactives)
        .map(t => (t._1, (t._2._2.getOrElse(""), t._2._1)))
        .filter(_._2._1 != "")

      //Etape 1 : on a RDD[(idProduct, (idClient, ca, quantity))]
      //Etape 2 : on leftOuterJoin avec RDD[(idProduct, famille, axe)]
      //Etape 3 : on transforme en RDD[(idProduct, idClient, ca, quantity, famille, axe)]
      //Etape 4 : on groupBy idClient
      //Etape 5 : on filter la seq sur axe == "PARFUM"
      //On a donc RDD[(idClient, Seq(idProduct, idClient, ca, quantity, famille, axe))]
      val idClient_idProduct_famille_ca_quantity_axe = idProduct_idClient_CA_quantity
        .leftOuterJoin(idProduct_famille_axe)
        .map(t => (t._1, t._2._1._1, t._2._1._2, t._2._1._3, t._2._2.getOrElse(("", ""))._1, t._2._2.getOrElse(("", ""))._2))
        .groupBy(_._2)
        .map(t => (t._1, t._2.filter(p => p._6 == "PARFUM")))


      //Etape 1 : on a RDD[(idClient, Seq(idProduct, idClient, ca, quantity, famille, axe))]
      //Etape 2 : on calcule quantity_famille_eq_"" / quantity_tot, on a RDD[(idClient, isQualified, Seq[(idProduct, idClient, ca, quantity, famille, axe)])]
      //Etape 3 : on filtre sur isQualified
      //Etape 4 : on transforme en RDD[(idClient, Seq[(idProduct, idClient, ca, quantity, famille, axe)])]
      val idClientQualifie_iProduct_famille_ca_quantity_axe = idClient_idProduct_famille_ca_quantity_axe
        .map(t => {
          val quantity_famille_eq_empty = t._2.filter(p => p._5 == "").map(p => p._4).sum
          val quantity_tot = t._2.map(p => p._4).sum
          val pourcent = quantity_famille_eq_empty.toDouble / quantity_tot.toDouble
          val isQualified = if(pourcent > 0.5d) 0 else 1
          (t._1, isQualified, t._2)
        })
        .filter(_._2 == 1)
        .map(t => (t._1, t._3))



      //Etape 1 : on a RDD[(idClient, Seq[(idProduct, idClient, ca, quantity, famille, axe)])]
      //Etape 2 : on groupBy le seq par famille et on calcule le ca tot
      //On a donc RDD[(idClient, ca_tot, Map[(famille, Seq[(idProduct, idClient, ca, quantity, famille, axe)]])]
      //Etape 3 : on calcule la somme du ca par famille
      //On a donc RDD[(idClient, ca_tot, Map[(famille, somme_ca)]

      val idClient_caTot_map_famille_ca = idClientQualifie_iProduct_famille_ca_quantity_axe
        .map(t => (t._1, t._2.map(_._3).sum, t._2.groupBy(_._5)))
        .map(t => (t._1, t._2, t._3.mapValues(seq => seq.map(prod => prod._3).sum)))

      //Etape 1 : on a RDD[(idClient, ca_tot, Map[(famille, somme_ca)]
      //Etape 2 : on transforme en RDD[(idClient, Map[(famille, score)]
      //Etape 3 : on transforme en RDD[(idClient, (score_AROMATIQUE, score_BOISE, score_CHYPRE, score_FLORAL, score_FOUGERE, score_HESPERIDE, score_ORIENTAL))]
      val idClient_scores_familles = idClient_caTot_map_famille_ca
        .map(t => (t._1, t._3.mapValues(p => p/t._2)))
        .map(t => {
        val score_AROMATIQUE = t._2.getOrElse("AROMATIQUE", 0f)
        val score_BOISE = t._2.getOrElse("BOISE", 0f)
        val score_CHYPRE = t._2.getOrElse("CHYPRE", 0f)
        val score_FLORAL = t._2.getOrElse("FLORAL", 0f)
        val score_FOUGERE = t._2.getOrElse("FOUG�RE", 0f) //FAIRE GAFFE AVEC CETTE ACCENT !!!!!!!!!!!!
        val score_HESPERIDE = t._2.getOrElse("HESPERIDE", 0f)
        val score_ORIENTAL = t._2.getOrElse("ORIENTAL", 0f)

        (t._1, score_AROMATIQUE, score_BOISE, score_CHYPRE, score_FLORAL, score_FOUGERE, score_HESPERIDE, score_ORIENTAL)
      })

      idClient_scores_familles

    }
    ////////////////////Fin familles olfactives/////////////////////
    ////////////////////Debut Marque émergente Maquillage///////////
    def get_notes_marque_emergente_maquillage()={
      val CA_QTE_PDT_DATE_POLOGNE = CA_QTE_PDT_DATE_POLOGNE_Fetcher.generate_table()
      val TABLE_PRODUIT_POLOGNE_VF = TABLE_PRODUIT_POLOGNE_VF_Fetcher.generate_table()
      //RDD[(idClient, idGender, age, idCardStatus, idSegmentRFM, List[(idTicket, pruchaseDate, List[(idProduct, quantity, price)])])]
      val lignes_ventes_fid_3_ans = extractPurchaseLines()


      //Etape 1 : on a RDD[product_complet]
      //Etape 2 : on transforme en RDD[(idProduct, product_complet)]
      val idproduct_productComplet = TABLE_PRODUIT_POLOGNE_VF
        .map(p => (p.product_id, p))

      //Etape 1 : on a RDD[Produit_date_quantity_ca]
      //Etape 2 : on transforme en RDD[(idProduct, Produit_date_quantity_ca)]
      //Etape 3 : on leftOuterJoin avec idProduct_productComplet
      //Etape 4 : on transforme en RDD[(produit_date_quantity_ca, product_complet)]
      //Etape 5 : on filter sur product_complet != null
      //Etape 6 : on filter sur axe == maquillage et market_code == emergent
      val produits_filter_axe_et_market_code = CA_QTE_PDT_DATE_POLOGNE
        .map(p => (p.product_id, p))
        .leftOuterJoin(idproduct_productComplet)
        .map(p => (p._2._1, p._2._2.getOrElse(null)))
        .filter(p => p._2 != null)
        .filter(p => p._2.axe == Tables_Constantes.PRODUITS_AXE_MAQUILLAGE && p._2.market_Code == Tables_Constantes.PRODUITS_MARKET_CODE_EMERGENT)


      case class BrandCode_date_quantity_ca_dateNMinus1_dateNMinus2_seqIdProduct(brand_code: String, date: DateTime, quantity: Int,
                                                                  CA: Double, dateNMinus1: DateTime, dateNMinus2: DateTime,
                                                                   IdProducts: Seq[String])

      //Etape 1 : on a RDD[(produit_date_quantity_ca, product_complet)]
      //Etape 2 : on transforme en RDD[(brand_code, date), (produit_date_quantity_ca, produit_complet))]
      //Etape 3 : on groupBy (brand_code, date)
      val sales_lines_with_dn1_and_dn2 = produits_filter_axe_et_market_code
        .map(p => ((p._2.brand_Code, p._1.date), (p._1, p._2)))
        .groupBy(_._1)
        .map(p => {
          val dn1 = p._1._2.minusYears(1)
          val dn2 = p._1._2.minusYears(2)
          val quantity = p._2.map(t => t._2._1.quantity).sum
          val ca = p._2.map(t => t._2._1.CA).sum
          val idProducts = p._2.map(t => t._2._1.product_id)
          BrandCode_date_quantity_ca_dateNMinus1_dateNMinus2_seqIdProduct(p._1._1, p._1._2, quantity, ca, dn1, dn2, idProducts)
        })



      //Etape 1 : on a RDD[BrandCode_date_quantity_ca_dateNMinus1_dateNMinus2_seqIdProduct]
      //Etape 2 : on transforme en RDD[(brand_code, date), is_emergent
      val brandCode_date_isEmergent = sales_lines_with_dn1_and_dn2
        .map(p => {
        val quantity_nMinus1 = sales_lines_with_dn1_and_dn2.filter(s => s.date.isAfter(p.dateNMinus1) && s.date.isBefore(p.date) && s.brand_code == p.brand_code).map(_.quantity).sum
        val ca_nMinus1 = sales_lines_with_dn1_and_dn2.filter(s => s.date.isAfter(p.dateNMinus1) && s.date.isBefore(p.date) && s.brand_code == p.brand_code).map(_.CA).sum

        val ca_nMinus2 = sales_lines_with_dn1_and_dn2.filter(s => s.date.isAfter(p.dateNMinus2) && s.date.isBefore(p.dateNMinus1) && s.brand_code == p.brand_code).map(_.CA).sum

        val evol = (ca_nMinus1/ca_nMinus2) - 1
        val is_emergent = Calcul_Notes_Constantes.is_marque_emergente_maquillage(quantity_nMinus1.toInt, evol, ca_nMinus1)
        
        ((p.brand_code, p.date), is_emergent)
      })

      //Etape 1 : on a RDD[(idClient, idGender, age, idCardStatus, idSegmentRFM, List[(idTicket, pruchaseDate, List[(idProduct, quantity, price)])])]
      //Etape 2 : on transforme en RDD[(idClient, List[(purchaseDate, idProduct, quantity, price)])] 
      //Etape 3 : on transforme en RDD[(idClient, purchaseDate, idProduct, quantity, price)]
      //Etape 4 : on transforme en RDD[(idProduct, (idCLient, purchaseDate, quantity, price))]
      //Etape 5 : on leftOuterJoin avec idProduct_productComplet
      //Etape 6 : on transforme en RDD[(idProduct, idCLient, purchaseDate, quantity, price, product_complet)]
      //Etape 7 : on filtre sur product_complet != null
      //Etape 8 : on transforme en RDD[(brand_code, purchaseDate), (idClient, quantity, price)]
      val brandCode_purchaseDate_idClient_quantity_price = lignes_ventes_fid_3_ans
        .map(t => (t._1, t._6.flatMap(p => p._3.map(prod => (p._2, prod._1, prod._2, prod._3)))))
        .flatMap(t => t._2.map(p => (t._1, p._1, p._2, p._3, p._4)))
        .map(t => (t._3, (t._1, StringToDate.from_format_claravista_to_datetime(t._2), t._4, t._5)))
        .leftOuterJoin(idproduct_productComplet)
        .map(t => (t._1, t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._2.getOrElse(null)))
        .filter(t => t._6 != null)
        .map(t => ((t._6.brand_Code, t._3), (t._2, t._4, t._5)))
      
      
      //Etape 1 : on a RDD[(brand_code, purchaseDate), (idClient, quantity, price)]
      //Etape 2 : on leftOuterJoin avec RDD[(brand_code, date), is_emergent
      //Etape 3 : on transforme en RDD[(idClient, quantity, price, is_emergent)]
      //Etape 4 : on filtre sur is_emergent != null
      //Etape 5 : on transforme en RDD[(idClient, ca, is_emergent)]
      //Etape 6 : on groupBy idClient
      //On a donc RDD[(idClient, Seq[(idClient, ca, is_emergent)])]
      val idClient_seq_ca_isEmergent = brandCode_purchaseDate_idClient_quantity_price
        .leftOuterJoin(brandCode_date_isEmergent)
        .map(t => (t._2._1._1, t._2._1._2, t._2._1._3, t._2._2.getOrElse(null)))
        .filter(t => t._4 != null)
        .map(t => (t._1, t._2.toDouble * t._3.toDouble, t._4.asInstanceOf[Boolean]))
        .groupBy(_._1)

      //Etape 1 : on a RDD[(idClient, Seq[(idClient, ca, is_emergent)])]
      //Etape 2 : on transforme en RDD[(idClient, ca_tot, Seq[(ca, is_emergent)])]
      //Etape 3 : on transforme en RDD[(idClient, note)]
      val idClient_note = idClient_seq_ca_isEmergent
        .map(t => (t._1, t._2.map(_._2).sum, t._2.map(p => (p._2, p._3))))
        .map(t => {
        val seq_prod_sum = t._3.map(p => p._1 * Calcul_Notes_Constantes.transform_boolean_to_int(p._2)).sum
        (t._1, seq_prod_sum/t._2)
      })

      idClient_note
    }
    ////////////////////Fin Marque émergente Maquillage/////////////
    ////////////////////Debut Marque émargent Soin//////////////////
    def get_notes_marque_emergente_soin()={
      val CA_QTE_PDT_DATE_POLOGNE = CA_QTE_PDT_DATE_POLOGNE_Fetcher.generate_table()
      val TABLE_PRODUIT_POLOGNE_VF = TABLE_PRODUIT_POLOGNE_VF_Fetcher.generate_table()
      //RDD[(idClient, idGender, age, idCardStatus, idSegmentRFM, List[(idTicket, pruchaseDate, List[(idProduct, quantity, price)])])]
      val lignes_ventes_fid_3_ans = extractPurchaseLines()


      //Etape 1 : on a RDD[product_complet]
      //Etape 2 : on transforme en RDD[(idProduct, product_complet)]
      val idproduct_productComplet = TABLE_PRODUIT_POLOGNE_VF
        .map(p => (p.product_id, p))

      //Etape 1 : on a RDD[Produit_date_quantity_ca]
      //Etape 2 : on transforme en RDD[(idProduct, Produit_date_quantity_ca)]
      //Etape 3 : on leftOuterJoin avec idProduct_productComplet
      //Etape 4 : on transforme en RDD[(produit_date_quantity_ca, product_complet)]
      //Etape 5 : on filter sur product_complet != null
      //Etape 6 : on filter sur axe == maquillage et market_code == emergent
      val produits_filter_axe_et_market_code = CA_QTE_PDT_DATE_POLOGNE
        .map(p => (p.product_id, p))
        .leftOuterJoin(idproduct_productComplet)
        .map(p => (p._2._1, p._2._2.getOrElse(null)))
        .filter(p => p._2 != null)
        .filter(p => p._2.axe == Tables_Constantes.PRODUITS_AXE_SOIN && p._2.market_Code == Tables_Constantes.PRODUITS_MARKET_CODE_EMERGENT)


      case class BrandCode_date_quantity_ca_dateNMinus1_dateNMinus2_seqIdProduct(brand_code: String, date: DateTime, quantity: Int,
                                                                                 CA: Double, dateNMinus1: DateTime, dateNMinus2: DateTime,
                                                                                 IdProducts: Seq[String])

      //Etape 1 : on a RDD[(produit_date_quantity_ca, product_complet)]
      //Etape 2 : on transforme en RDD[(brand_code, date), (produit_date_quantity_ca, produit_complet))]
      //Etape 3 : on groupBy (brand_code, date)
      val sales_lines_with_dn1_and_dn2 = produits_filter_axe_et_market_code
        .map(p => ((p._2.brand_Code, p._1.date), (p._1, p._2)))
        .groupBy(_._1)
        .map(p => {
        val dn1 = p._1._2.minusYears(1)
        val dn2 = p._1._2.minusYears(2)
        val quantity = p._2.map(t => t._2._1.quantity).sum
        val ca = p._2.map(t => t._2._1.CA).sum
        val idProducts = p._2.map(t => t._2._1.product_id)
        BrandCode_date_quantity_ca_dateNMinus1_dateNMinus2_seqIdProduct(p._1._1, p._1._2, quantity, ca, dn1, dn2, idProducts)
      })



      //Etape 1 : on a RDD[BrandCode_date_quantity_ca_dateNMinus1_dateNMinus2_seqIdProduct]
      //Etape 2 : on transforme en RDD[(brand_code, date), is_emergent
      val brandCode_date_isEmergent = sales_lines_with_dn1_and_dn2
        .map(p => {
        val quantity_nMinus1 = sales_lines_with_dn1_and_dn2.filter(s => s.date.isAfter(p.dateNMinus1) && s.date.isBefore(p.date) && s.brand_code == p.brand_code).map(_.quantity).sum
        val ca_nMinus1 = sales_lines_with_dn1_and_dn2.filter(s => s.date.isAfter(p.dateNMinus1) && s.date.isBefore(p.date) && s.brand_code == p.brand_code).map(_.CA).sum

        val ca_nMinus2 = sales_lines_with_dn1_and_dn2.filter(s => s.date.isAfter(p.dateNMinus2) && s.date.isBefore(p.dateNMinus1) && s.brand_code == p.brand_code).map(_.CA).sum

        val evol = (ca_nMinus1/ca_nMinus2) - 1
        val is_emergent = Calcul_Notes_Constantes.is_marque_emergente_soin(quantity_nMinus1.toInt, evol, ca_nMinus1)

        ((p.brand_code, p.date), is_emergent)
      })

      //Etape 1 : on a RDD[(idClient, idGender, age, idCardStatus, idSegmentRFM, List[(idTicket, pruchaseDate, List[(idProduct, quantity, price)])])]
      //Etape 2 : on transforme en RDD[(idClient, List[(purchaseDate, idProduct, quantity, price)])]
      //Etape 3 : on transforme en RDD[(idClient, purchaseDate, idProduct, quantity, price)]
      //Etape 4 : on transforme en RDD[(idProduct, (idCLient, purchaseDate, quantity, price))]
      //Etape 5 : on leftOuterJoin avec idProduct_productComplet
      //Etape 6 : on transforme en RDD[(idProduct, idCLient, purchaseDate, quantity, price, product_complet)]
      //Etape 7 : on filtre sur product_complet != null
      //Etape 8 : on transforme en RDD[(brand_code, purchaseDate), (idClient, quantity, price)]
      val brandCode_purchaseDate_idClient_quantity_price = lignes_ventes_fid_3_ans
        .map(t => (t._1, t._6.flatMap(p => p._3.map(prod => (p._2, prod._1, prod._2, prod._3)))))
        .flatMap(t => t._2.map(p => (t._1, p._1, p._2, p._3, p._4)))
        .map(t => (t._3, (t._1, StringToDate.from_format_claravista_to_datetime(t._2), t._4, t._5)))
        .leftOuterJoin(idproduct_productComplet)
        .map(t => (t._1, t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._2.getOrElse(null)))
        .filter(t => t._6 != null)
        .map(t => ((t._6.brand_Code, t._3), (t._2, t._4, t._5)))


      //Etape 1 : on a RDD[(brand_code, purchaseDate), (idClient, quantity, price)]
      //Etape 2 : on leftOuterJoin avec RDD[(brand_code, date), is_emergent
      //Etape 3 : on transforme en RDD[(idClient, quantity, price, is_emergent)]
      //Etape 4 : on filtre sur is_emergent != null
      //Etape 5 : on transforme en RDD[(idClient, ca, is_emergent)]
      //Etape 6 : on groupBy idClient
      //On a donc RDD[(idClient, Seq[(idClient, ca, is_emergent)])]
      val idClient_seq_ca_isEmergent = brandCode_purchaseDate_idClient_quantity_price
        .leftOuterJoin(brandCode_date_isEmergent)
        .map(t => (t._2._1._1, t._2._1._2, t._2._1._3, t._2._2.getOrElse(null)))
        .filter(t => t._4 != null)
        .map(t => (t._1, t._2.toDouble * t._3.toDouble, t._4.asInstanceOf[Boolean]))
        .groupBy(_._1)

      //Etape 1 : on a RDD[(idClient, Seq[(idClient, ca, is_emergent)])]
      //Etape 2 : on transforme en RDD[(idClient, ca_tot, Seq[(ca, is_emergent)])]
      //Etape 3 : on transforme en RDD[(idClient, note)]
      val idClient_note = idClient_seq_ca_isEmergent
        .map(t => (t._1, t._2.map(_._2).sum, t._2.map(p => (p._2, p._3))))
        .map(t => {
        val seq_prod_sum = t._3.map(p => p._1 * Calcul_Notes_Constantes.transform_boolean_to_int(p._2)).sum
        (t._1, seq_prod_sum/t._2)
      })

      idClient_note
    }
    ////////////////////Fin Marque émergente Soin///////////////////


    //Etape 1 : on récupère le RDD des lignes de vente (id, idGender, age, idCardStatus, idSegmentRFM, List[(idTicket, pruchaseDate, List[(idProduct, quantity, price)])])
    //Etape 2 : on transformer en RDD[(idClient, <something>)]
    //Etape 3 : le deuxième élément est Tools.concatener(<something>)
    //Etape 4 : le something est List[List[idProduct, quantity*price]]
    //Etape 5 : on a donc un RDD[(idClient, List[(idProduct, quantity*price)])]
//    val client_list_prod = extractPurchaseLines().map(x => {
//      (x._1,Tools.concatener(x._6.map(_._3.map(y => (y._1, y._2 * y._3)))))
//    })
//
//    //Etape 1 : On a (RDD[(idCLient, List[(idProduct, quantity*price)]))
//    //Etape 2 : on transforme en RDD[(idProduct, <something>)]
//    //Etape 3 : le <something> est :
//    //Etape 3 a : on prends RDD[(idProduct, Array(flags et autres)]
//    //Etape 3 b : on filtre sur
//    val temp2 = temp
//      .map(x => (x._1,
//        calculateNotesProd.filter(_._1 == x._2.head).first()._2  +:  x._2.tail match {
//          case a :List[(Double, Array[Any])] => a
//          case _ => throw new Exception
//    }))



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

    //TODO : monthResultProductLines est pas ventes_non_fid en fait
    //TODO : faire les imports avec les tables csv
    //TODO : faire en sorte que les paramètres puissent être changés facilement
    //TODO : refractor...
    //TODO : verif que la table nouveaute est bien calculée


    calculateNotesClient

    println(Tools.concatener(List(List(1, 2, 3), List(4, 5, 6), List(7, 8, 9))))
    println
    println("blabla")
    println

  }

}

