package fetcher

import common.sparkSetting._

/**
 * Created with IntelliJ IDEA.
 * User: spark
 * Date: 7/30/14
 * Time: 2:36 PM
 */
object csvFetcher{


  def readData(path: String, has_header: Boolean, delimiter:String, string_delimiter: String) = {
    var rdd = sparkContext.textFile(path)
    if(has_header){
      val first = rdd.first
      rdd = rdd.filter(s => s != first)
    }
    rdd.map(s => {
        var arr_res = Array[String]()

        val s_split = s.split(delimiter, -1)
        var res_temp = ""
        for(sres <- s_split){
          if(sres == ""){
            if(res_temp == ""){
              arr_res = arr_res :+ sres
            }else{
              res_temp = res_temp + sres + delimiter
            }
          }else if(sres(0).toString == string_delimiter){
            if(sres(sres.length - 1).toString == string_delimiter){
              arr_res = arr_res :+ sres.substring(1, sres.size - 1)
            }else{
            res_temp = res_temp + sres.substring(1) + delimiter
            }
          }else if(sres(sres.length - 1).toString == string_delimiter){
            arr_res = arr_res :+ res_temp + sres.substring(0, sres.size - 1)
            res_temp = ""
          }else if(res_temp != ""){
            res_temp = res_temp + sres + delimiter
          }else{
            arr_res = arr_res :+ sres
          }

        }
        arr_res
    })
  }
}
