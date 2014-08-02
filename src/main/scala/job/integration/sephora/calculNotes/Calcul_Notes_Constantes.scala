package job.integration.sephora.calculNotes

/**
 * Created with IntelliJ IDEA.
 * User: spark
 * Date: 8/2/14
 * Time: 6:17 PM
 */
object Calcul_Notes_Constantes{

  val SEUIL_MARQUE_EMERGENTE_MAQUILLAGE_QUANTITE_VENDU_N_MINUS_1 = 50
  val SEUIL_MARQUE_EMERGENTE_MAQUILLAGE_EVOL_CA = -0.2
  val SEUIL_MARQUE_EMERGENTE_MAQUILLAGE_CA_N_MINUS_1 = 3100000

  def is_marque_emergente_maquillage(quantity_vendu_n_minus_1: Int, evol_ca: Double, ca_n_minus_1: Double): Boolean={
    quantity_vendu_n_minus_1 > SEUIL_MARQUE_EMERGENTE_MAQUILLAGE_QUANTITE_VENDU_N_MINUS_1 &&
       evol_ca > SEUIL_MARQUE_EMERGENTE_MAQUILLAGE_EVOL_CA &&
       ca_n_minus_1 < SEUIL_MARQUE_EMERGENTE_MAQUILLAGE_CA_N_MINUS_1
  }

  val SEUIL_MARQUE_EMERGENTE_SOIN_QUANTITE_VENDU_N_MINUS_1 = 50
  val SEUIL_MARQUE_EMERGENTE_SOIN_EVOL_CA = -0.2
  val SEUIL_MARQUE_EMERGENTE_SOIN_CA_N_MINUS_1 = 4900000

  def is_marque_emergente_soin(quantity_vendu_n_minus_1: Int, evol_ca: Double, ca_n_minus_1: Double): Boolean={
    quantity_vendu_n_minus_1 > SEUIL_MARQUE_EMERGENTE_SOIN_QUANTITE_VENDU_N_MINUS_1 &&
      evol_ca > SEUIL_MARQUE_EMERGENTE_SOIN_EVOL_CA &&
      ca_n_minus_1 < SEUIL_MARQUE_EMERGENTE_SOIN_CA_N_MINUS_1
  }

  /**
   * Retourne 1 pour true, 0 pour false
   * @param b
   * @return 1 pour true, 0 pour false
   */
  def transform_boolean_to_int(b: Boolean): Int={
    if(b) 1 else 0
  }
}
