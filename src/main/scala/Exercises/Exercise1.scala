package Exercises
import scala.io.Source
import shapeless.ops._
import shapeless._
import scala.collection.GenTraversable
import shapeless._
import shapeless.HList._

object Exercise1 {
  def main(args: Array[String]): Unit = {

    val usa_barley = Source.fromFile("usa_barley.txt").getLines.drop(1).toList
    val wld_barley = Source.fromFile("world_barley.txt").getLines.drop(1).toList
//    usa_barley.foreach(println)
    //usa_barley.foreach(println)

    case class barley(year:String, harvest:Long, Yield:Float, Production:Long, Import:Long,Exports:Long,consumption:Long,industrialuse:Long,use:Long,stocks:Long)

    def parseBarley(str: String):barley = {
      val x = str.split(',')
      barley(x(0),x(1).toLong,x(2).toFloat,x(3).toLong,x(4).toLong,x(5).toLong,x(6).toLong,x(7).toLong,x(8).toLong,x(9).toLong)
    }

    val usabar = usa_barley.map(parseBarley).map(barley => (barley.year, barley.harvest))
    //usabar.foreach(println)
    val wldbar = wld_barley.map(parseBarley).map(barley => (barley.year, barley.harvest))
    //wldbar.foreach(println)

   val barleyData =  usabar.zip(wldbar).map{
      case(x,y) => (x._1.split('/')(0), y._2,(x._2.toDouble/y._2.toDouble)*100)
    }
//    barleyData.foreach(println)



    //beef Data
    val usa_beef = Source.fromFile("usa_beef.txt").getLines.drop(1).filter(line => line.contains("--")).map(line => line.replace("--","0")).toList
    //usa_beef.foreach(println)
    val wld_beef = Source.fromFile("world_beef.txt").getLines.drop(1).toList
    //wld_beef.foreach(println)

    case class beef(year:String,	Slaughter:Long,	Yield:Long,	Production:Long,	Imports:Long,	Exports:Long,	consumption:Long,	stocks:Long)

    def parseBeef(str: String):beef ={
      val x = str.split("\t")
      beef(x(0), x(1).toLong, x(2).toLong, x(3).toLong,x(4).toLong, x(5).toLong, x(6).toLong, x(7).toLong)
    }

    val usabeef = usa_beef.map(parseBeef).map(beef => (beef.year, beef.Slaughter))
//    usabeef.foreach(println)
    val wldbeef = wld_beef.map(parseBeef).map(beef => (beef.year, beef.Slaughter))
//    wldbeef.foreach(println)

    val beefData = usabeef.zip(wldbeef).map{
      case(x,y) => (x._1, y._2,(x._2.toDouble/y._2.toDouble)*100)
    }
    //beefData.foreach(println)

    //corn Data
    val usa_corn = Source.fromFile("usa_corn.txt").getLines.drop(1).toList
    val wld_corn = Source.fromFile("world_corn.txt").getLines.drop(1).toList
    //    usa_corn.foreach(println)
    //usa_corn.foreach(println)

    case class corn(year:String, harvest:Long, Yield:Float, Production:Long, Import:Long,Exports:Long,consumption:Long,industrialuse:Long,use:Long,stocks:Long)

    def parsecorn(str: String):corn = {
      val x = str.split("\t")
      corn(x(0),x(1).toLong,x(2).toFloat,x(3).toLong,x(4).toLong,x(5).toLong,x(6).toLong,x(7).toLong,x(8).toLong,x(9).toLong)
    }

    val usacrn = usa_corn.map(parsecorn).map(corn => (corn.year, corn.harvest))
    //usacrn.foreach(println)
    val wldcrn = wld_corn.map(parsecorn).map(corn => (corn.year, corn.harvest))
    //wldcrn.foreach(println)

    val cornData =  usacrn.zip(wldcrn).map{
      case(x,y) => (x._1.split('/')(0), y._2,(x._2.toDouble/y._2.toDouble)*100)
    }
//        cornData.foreach(println)

    //cotton Data
    val usa_cotton = Source.fromFile("usa_cotton.txt").getLines.drop(1).toList
    val wld_cotton = Source.fromFile("world_cotton.txt").getLines.drop(1).toList
    //    usa_cotton.foreach(println)
    //usa_cotton.foreach(println)

    case class cotton(year:String,	harvest:Long,	Yield:Long,	Production:Long,	Imports:Long,	Exports:Long,	consumption:Long,	loss:Long,	stocks:Long)

    def parsecotton(str: String):cotton = {
      val x = str.split("\t")
      cotton(x(0),x(1).toLong,x(2).toLong,x(3).toLong,x(4).toLong,x(5).toLong,x(6).toLong,x(7).toLong,x(8).toLong)
    }

    val usactn = usa_cotton.map(parsecotton).map(cotton => (cotton.year, cotton.harvest))
    //usactn.foreach(println)
    val wldctn = wld_cotton.map(parsecotton).map(cotton => (cotton.year, cotton.harvest))
    //wldctn.foreach(println)

    val cottonData =  usactn.zip(wldctn).map{
      case(x,y) => (x._1.split('/')(0), y._2,(x._2.toDouble/y._2.toDouble)*100)
    }

//    cottonData.foreach(println)

    //pork Data

    val usa_pork = Source.fromFile("usa_pork.txt").getLines.drop(1).toList
    //usa_pork.foreach(println)
    val wld_pork = Source.fromFile("world_pork.txt").getLines.drop(1).toList
    //wld_pork.foreach(println)

    case class pork(year:String,	Slaughter:Long,	Yield:Long,	Production:Long,	Imports:Long,	Exports:Long,	consumption:Long,	stocks:Long)

    def parsepork(str: String):pork ={
      val x = str.split("\t")
      pork(x(0), x(1).toLong, x(2).toLong, x(3).toLong,x(4).toLong, x(5).toLong, x(6).toLong, x(7).toLong)
    }

    val usapork = usa_pork.map(parsepork).map(pork => (pork.year, pork.Slaughter))
    //    usapork.foreach(println)
    val wldpork = wld_pork.map(parsepork).map(pork => (pork.year, pork.Slaughter))
    //    wldpork.foreach(println)

    val porkData = usapork.zip(wldpork).map{
      case(x,y) => (x._1, y._2,(x._2.toDouble/y._2.toDouble)*100)
    }
//    porkData.foreach(println)

    //poultry Data
    val usa_poultry = Source.fromFile("usa_poultry.txt").getLines.drop(1).toList
    //usa_poultry.foreach(println)
    val wld_poultry = Source.fromFile("world_poultry.txt").getLines.drop(1).toList
    //wld_poultry.foreach(println)

    case class poultry(year:String,	Production:Long,	Imports:Long,	Exports:Long,	consumption:Long,	stocks:Long)

    def parsepoultry(str: String):poultry ={
      val x = str.split("\t")
      poultry(x(0), x(1).toLong, x(2).toLong, x(3).toLong,x(4).toLong, x(5).toLong)
    }

    val usapoultry = usa_poultry.map(parsepoultry).map(poultry => (poultry.year, poultry.Production))
    //    usapoultry.foreach(println)
    val wldpoultry = wld_poultry.map(parsepoultry).map(poultry => (poultry.year, poultry.Production))
    //    wldpoultry.foreach(println)

    val poultryData = usapoultry.zip(wldpoultry).map{
      case(x,y) => (x._1, y._2,(x._2.toDouble/y._2.toDouble)*100)
    }
//    poultryData.foreach(println)

    //rice Data
    val usa_rice = Source.fromFile("usa_rice.txt").getLines.drop(1).toList
    val wld_rice = Source.fromFile("world_rice.txt").getLines.drop(1).toList
    //    usa_rice.foreach(println)
    //usa_rice.foreach(println)

    case class rice(year:String,harvest:Long,Yield:Float,Production:Long,Imports:Long,Exports:Long,consumption:Long,stocks:Long)


    def parserice(str: String):rice = {
      val x = str.split("\t")
      rice(x(0),x(1).toLong,x(2).toFloat,x(3).toLong,x(4).toLong,x(5).toLong,x(6).toLong,x(7).toLong)
    }

    val usarice = usa_rice.map(parserice).map(rice => (rice.year, rice.harvest))
    //usarice.foreach(println)
    val wldrice = wld_rice.map(parserice).map(rice => (rice.year, rice.harvest))
    //wldrice.foreach(println)

    val riceData =  usarice.zip(wldrice).map{
      case(x,y) => (x._1.split('/')(0), y._2,(x._2.toDouble/y._2.toDouble)*100)
    }
//    riceData.foreach(println)

    //sorghum Data
    val usa_sorghum = Source.fromFile("usa_sorghum.txt").getLines.drop(1).toList
    val wld_sorghum = Source.fromFile("world_sorghum.txt").getLines.drop(1).toList
    //    usa_sorghum.foreach(println)
    //usa_sorghum.foreach(println)

    case class sorghum(year:String, harvest:Long, Yield:Float, Production:Long, Import:Long,Exports:Long,consumption:Long,industrialuse:Long,use:Long,stocks:Long)

    def parsesorghum(str: String):sorghum = {
      val x = str.split("\t")
      sorghum(x(0),x(1).toLong,x(2).toFloat,x(3).toLong,x(4).toLong,x(5).toLong,x(6).toLong,x(7).toLong,x(8).toLong,x(9).toLong)
    }

    val usasorghum = usa_sorghum.map(parsesorghum).map(sorghum => (sorghum.year, sorghum.harvest))
    //usasorghum.foreach(println)
    val wldsorghum = wld_sorghum.map(parsesorghum).map(sorghum => (sorghum.year, sorghum.harvest))
    //wldsorghum.foreach(println)

    val sorghumData =  usasorghum.zip(wldsorghum).map{
      case(x,y) => (x._1.split('/')(0), y._2,(x._2.toDouble/y._2.toDouble)*100)
    }
//        sorghumData.foreach(println)

    //soybeanmeal Data
    val usa_soybeanmeal = Source.fromFile("usa_soybeanmeal.txt").getLines.drop(1).toList
    val wld_soybeanmeal = Source.fromFile("world_soybeanmeal.txt").getLines.drop(1).toList
    //    usa_soybeanmeal.foreach(println)
    //usa_soybeanmeal.foreach(println)

    case class soybeanmeal(year:String,Crush:Long,rate:Float,Production:Long,Imports:Long,Exports:Long,consumption:Long,otheruse:Long,use:Long,stocks:Long)

    def parsesoybeanmeal(str: String):soybeanmeal = {
      val x = str.split("\t")
      soybeanmeal(x(0),x(1).toLong,x(2).toFloat,x(3).toLong,x(4).toLong,x(5).toLong,x(6).toLong,x(7).toLong,x(8).toLong,x(9).toLong)
    }

    val usasoybeanmeal = usa_soybeanmeal.map(parsesoybeanmeal).map(soybeanmeal => (soybeanmeal.year, soybeanmeal.Production))
    //usasoybeanmeal.foreach(println)
    val wldsoybeanmeal = wld_soybeanmeal.map(parsesoybeanmeal).map(soybeanmeal => (soybeanmeal.year, soybeanmeal.Production))
    //wldsoybeanmeal.foreach(println)

    val soybeanmealData =  usasoybeanmeal.zip(wldsoybeanmeal).map{
      case(x,y) => (x._1.split('/')(0), y._2,(x._2.toDouble/y._2.toDouble)*100)
    }
//        soybeanmealData.foreach(println)

    //soybeanoil Data
    val usa_soybeanoil = Source.fromFile("usa_soybeanoil.txt").getLines.drop(1).filter(line => line.contains("--")).map(line => line.replace("--","0")).toList
    val wld_soybeanoil = Source.fromFile("world_soybeanoil.txt").getLines.drop(1).toList
    //    usa_soybeanoil.foreach(println)
    //usa_soybeanoil.foreach(println)

    case class soybeanoil(year:String,Crush:Long,rate:Float,Production:Long,Imports:Long,Exports:Long,consumption:Long,use:Long,otheruse:Long,stocks:Long)

    def parsesoybeanoil(str: String):soybeanoil = {
      val x = str.split("\t")
      soybeanoil(x(0),x(1).toLong,x(2).toFloat,x(3).toLong,x(4).toLong,x(5).toLong,x(6).toLong,x(7).toLong,x(8).toLong,x(9).toLong)
    }

    val usasoybeanoil = usa_soybeanoil.map(parsesoybeanoil).map(soybeanoil => (soybeanoil.year, soybeanoil.Production))
    //usasoybeanoil.foreach(println)
    val wldsoybeanoil = wld_soybeanoil.map(parsesoybeanoil).map(soybeanoil => (soybeanoil.year, soybeanoil.Production))
    //wldsoybeanoil.foreach(println)

    val soybeanoilData =  usasoybeanoil.zip(wldsoybeanoil).map{
      case(x,y) => (x._1.split('/')(0), y._2,(x._2.toDouble/y._2.toDouble)*100)
    }
//        soybeanoilData.foreach(println)

    //soybeens Data
    val usa_soybeens = Source.fromFile("usa_soybeens.txt").getLines.drop(1).toList
    val wld_soybeens = Source.fromFile("world_soybeens.txt").getLines.drop(1).toList
    //    usa_soybeens.foreach(println)
    //usa_soybeens.foreach(println)

    case class soybeens(year:String,harvest:Long,Yield:Float,Production:Long,Import:Long,Exports:Long,consumption:Long,use:Long,waste:Long,Crush:Long,stocks:Long)


    def parsesoybeens(str: String):soybeens = {
      val x = str.split("\t")
      soybeens(x(0),x(1).toLong,x(2).toFloat,x(3).toLong,x(4).toLong,x(5).toLong,x(6).toLong,x(7).toLong,x(8).toLong,x(9).toLong,x(10).toLong)
    }

    val usasoybeens = usa_soybeens.map(parsesoybeens).map(soybeens => (soybeens.year, soybeens.harvest))
    //usasoybeens.foreach(println)
    val wldsoybeens = wld_soybeens.map(parsesoybeens).map(soybeens => (soybeens.year, soybeens.harvest))
    //wldsoybeens.foreach(println)

    val soybeensData =  usasoybeens.zip(wldsoybeens).map{
      case(x,y) => (x._1.split('/')(0), y._2,(x._2.toDouble/y._2.toDouble)*100)
    }
//        soybeensData.foreach(println)

    //wheat Data
    val usa_wheat = Source.fromFile("usa_wheat.txt").getLines.drop(1).toList
    val wld_wheat = Source.fromFile("world_wheat.txt").getLines.drop(1).toList
    //    usa_wheat.foreach(println)
    //usa_wheat.foreach(println)

    case class wheat(year:String, harvest:Long, Yield:Float, Production:Long, Import:Long,Exports:Long,consumption:Long,industrialuse:Long,use:Long,stocks:Long)

    def parsewheat(str: String):wheat = {
      val x = str.split("\t")
      wheat(x(0),x(1).toLong,x(2).toFloat,x(3).toLong,x(4).toLong,x(5).toLong,x(6).toLong,x(7).toLong,x(8).toLong,x(9).toLong)
    }

    val usawheat = usa_wheat.map(parsewheat).map(wheat => (wheat.year, wheat.harvest))
    //usawheat.foreach(println)
    val wldwheat = wld_wheat.map(parsewheat).map(wheat => (wheat.year, wheat.harvest))
    //wldwheat.foreach(println)

    val wheatData =  usawheat.zip(wldwheat).map{
      case(x,y) => (x._1.split('/')(0), y._2,(x._2.toDouble/y._2.toDouble)*100)
    }
//        wheatData.foreach(println)

    val x = barleyData.zip(
      beefData.zip(
        cornData.zip(
          cottonData.zip(
            porkData.zip(
              poultryData.zip(
                riceData.zip(
                  sorghumData.zip(
                    soybeanmealData.zip(
                      soybeanoilData.zip(
                        soybeensData.zip(
                          wheatData)))))))))))

    val y = x.map{
      case((a),((b),((c),((d),((e),((f),((g),((h),((i),((j),((k),((l))))))))))))) => List(a._1,a._2,a._3,b._2,b._3,c._2,c._3,d._2,d._3,e._2,e._3,f._2,f._3,g._2,g._3,h._2,h._3,i._2,i._3,j._2,j._3,k._2,k._3,l._2,l._3)
    }

//   y.toHList[String::Long::Double::Long::Double::Long::Double::Long::Double::Long::Double::Long::Double::Long::Double::Long::Double::Long::Double::Long::Double::Long::Double::Long::Double::HNil]
////
//    def preData(list: List[Any]):List[Any] ={List("year",  "world_barley", "usa_barleyharvest_contribution: Double,world_beef:Long, usa_beef_slaughter_contribution","world_corn", "usa_corn_harvest_contribution:Double,world_cotton:Long, usa_cotton_harvest_contribution:Double,world_pork:Long, usa_pork_slaughter_contribution:Double,world_poultry:Long, usa_poultry_production_contribution:Double,
//                       world_rice:Long, usa_rice_harvest_contribution:Double,world_sorghum:Long, usa_sorghum_harvest_contribution:Double,world_soybeanmeal:Long, usa_soybeanmeal_production_contribution:Double,world_soybeanoil:Long, usa_soybeanoil_production_contribution:Double,world_soybeens:Long, usa_soybeens_harvest_contribution:Double,
//                       world_wheat:Long, usa_wheat_harvest_contribution:Double)}
//    def parseData(list: List[String]):preData ={
//      val x = list.toString().split(",")
//      preData(x(0),x(1).toLong,x(2).toDouble,x(3).toLong,x(4).toDouble,x(5).toLong,x(6).toDouble,x(7).toLong,x(8).toDouble,x(9).toLong,x(10).toDouble,x(11).toLong,x(12).toDouble,x(13).toLong,x(14).toDouble,x(15).toLong,x(16).toDouble,x(17).toLong,x(18).toDouble,x(19).toLong,x(20).toDouble,x(21).toLong,x(22).toDouble,x(23).toLong,x(24).toDouble)
//    }

//    y.flatten.map(parseData).map(data => (data.year,data.world_barley,data.usa_barleyharvest_contribution,data.world_beef,data. usa_beef_slaughter_contribution,data.world_corn,data. usa_corn_harvest_contribution,data.world_cotton,data. usa_cotton_harvest_contribution,data.world_pork,data. usa_pork_slaughter_contribution,data.world_poultry,data. usa_poultry_production_contribution,data.world_rice,data. usa_rice_harvest_contribution,data.world_sorghum,data. usa_sorghum_harvest_contribution,data.world_soybeanmeal,
//      data. usa_soybeanmeal_production_contribution,data.world_soybeanoil,data. usa_soybeanoil_production_contribution,data.world_soybeens,data.world_wheat,data. usa_wheat_harvest_contribution)
//    )

    val finalData = y
    finalData.foreach(println)







  }


}
