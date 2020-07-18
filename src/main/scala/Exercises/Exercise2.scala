package Exercises

import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType}
import org.apache.spark.sql.functions._



object Exercise2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Exercise2")
    val sc = new SparkContext(conf)
    val spark =SparkSession.builder().appName("Exercises2").getOrCreate()
    import spark.implicits._

    //barley Data
    val wldbarley = spark.read.format("csv").option("header","true").load("world_barley.txt")
    //wldbarley.show()
    val usbarley = spark.read.format("csv").option("header","true").load("usa_barley.txt")
    //usbarley.show()

    val df_wldbrl = wldbarley.as("wldbrl")
    val df_usbrl = usbarley.as("usbrl")

    val toInt    = udf[Int, String]( _.toInt)
    val toDouble = udf[Double, String]( _.toDouble)

    val barley_join = df_wldbrl.join(df_usbrl, df_wldbrl("year")===df_usbrl("year"), "fullouter").select(("wldbrl.year"),"usbrl.harvest","wldbrl.harvest").toDF("year_col","us_barleyharvest", "wld_barleyharvest")
    val compbarley = barley_join.withColumn("us", toDouble(barley_join("us_barleyharvest"))).withColumn("wld", toDouble(barley_join("wld_barleyharvest")))
    val barleyData = compbarley.withColumn("comparison_barley", toDouble((compbarley("us")/compbarley("wld"))*100)).select("year_col","wld_barleyharvest","comparison_barley").orderBy("year_col")

    //beef Data
    val wldbeef = spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("world_beef.txt")
    val usbeef =  spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("usa_beef.txt")
    val df_wldbeef = wldbeef.as("wldbef")
    val df_usbeef = usbeef.as("usbef")
    val beef_join = df_wldbeef.join(df_usbeef, df_wldbeef("year")===df_usbeef("year"), "fullouter").select("wldbef.year", "usbef.Slaughter", "wldbef.Slaughter").toDF("year_col","us_beefslaughter","wld_beefslaughter")
    val compbeef = beef_join.withColumn("usbeefslaughter", beef_join("us_beefslaughter").cast(DoubleType)).withColumn("wldbeefslaughter", beef_join("wld_beefslaughter").cast(DoubleType)).na.fill(0)
    val beefData = compbeef.withColumn("comparison_beef", toDouble((compbeef("usbeefslaughter")/compbeef("wldbeefslaughter"))*100)).select("year_col","wldbeefslaughter","comparison_beef").orderBy("year_col")


    //val join_df=beefData.join(barleyData, beefData("year_col")===barleyData("year_col"),"outer").drop(beefData("year_col")).select("year_col","wldbeefslaughter","comparison_beef","wld_barleyharvest","comparison_barley").orderBy("year_col")
    //join_df.withColumnRenamed("comparison_beef","usa_beef_contribution%").show()

//    barleyData.join(beef_join, barleyData("year_col")===beefData("year_col"), "fullouter").show()
//
    //corn Data
    val wldcorn = spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("world_corn.txt")
    val uscorn = spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("usa_corn.txt")
    val df_wldcorn = wldcorn.as("wldcrn")
    val df_uscorn = uscorn.as("uscrn")
    val corn_join = df_wldcorn.join(df_uscorn, df_wldcorn("year")===df_uscorn("year"), "fullouter").select("wldcrn.year", "uscrn.harvest", "wldcrn.harvest").toDF("year_col","us_crnharvest","wld_crnharvest")
    val compcorn = corn_join.withColumn("uscornharvest", toDouble(corn_join("us_crnharvest"))).withColumn("wldcornharvest", toDouble(corn_join("wld_crnharvest")))
    val cornData = compcorn.withColumn("comparison_corn", toDouble((compcorn("uscornharvest")/compcorn("wldcornharvest"))*100)).select("year_col","wldcornharvest","comparison_corn").orderBy("year_col")

    //barleyData.join(cornData,barleyData("year_col")===cornData("year_col"),"fullouter").drop(cornData("year_col")).orderBy("year_col").show()




    //cotton Data
    val wldcotton = spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("world_cotton.txt")
    val uscotton = spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("usa_cotton.txt")
    val df_wldcotton = wldcotton.as("wldctn")
    val df_uscotton = uscotton.as("usctn")
    val cotton_join = df_wldcotton.join(df_uscotton, df_wldcotton("year")===df_uscotton("year"), "fullouter").select("wldctn.year", "usctn.harvest", "wldctn.harvest").toDF("year_col","us_ctnharvest","wld_ctnharvest")
    val compcotton = cotton_join.withColumn("uscottonharvest", toDouble(cotton_join("us_ctnharvest"))).withColumn("wldcottonharvest", toDouble(cotton_join("wld_ctnharvest")))
    val cottonData = compcotton.withColumn("comparison_cotton", toDouble((compcotton("uscottonharvest")/compcotton("wldcottonharvest"))*100)).select("year_col","wldcottonharvest","comparison_cotton").orderBy("year_col")

    //pork Data
    val wldpork = spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("world_pork.txt")
    val uspork =  spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("usa_pork.txt")
    val df_wldpork = wldpork.as("wldprk")
    val df_uspork = uspork.as("usprk")
    val pork_join = df_wldpork.join(df_uspork, df_wldpork("year")===df_uspork("year"), "fullouter").select("wldprk.year", "usprk.Slaughter", "wldprk.Slaughter").toDF("year_col","us_slaughter","wld_slaughter")
    val comppork = pork_join.withColumn("usporkslaughter", pork_join("us_Slaughter").cast(DoubleType)).withColumn("wldporkslaughter", pork_join("wld_slaughter").cast(DoubleType)).select("year_col","usporkslaughter","wldporkslaughter")
    val porkData = comppork.withColumn("comparison_pork", toDouble((comppork("usporkslaughter")/comppork("wldporkslaughter"))*100)).select("year_col","wldporkslaughter","comparison_pork").orderBy("year_col")

    //poultry Data
    val wldpoultry = spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("world_poultry.txt")
    val uspoultry =  spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("usa_poultry.txt")
    val df_wldpoultry = wldpoultry.as("wldplrt")
    val df_uspoultry = uspoultry.as("usplrt")
    val poultry_join = df_wldpoultry.join(df_uspoultry, df_wldpoultry("year")===df_uspoultry("year"), "fullouter").select("wldplrt.year", "usplrt.production", "wldplrt.production").toDF("year_col","us_production","wld_production")
    val comppoultry = poultry_join.withColumn("uspoultryproduction", poultry_join("us_production").cast(DoubleType)).withColumn("wldpoultryproduction", poultry_join("wld_production").cast(DoubleType)).select("year_col","uspoultryproduction","wldpoultryproduction")
    val poultryData = comppoultry.withColumn("comparison_poultry", toDouble((comppoultry("uspoultryproduction")/comppoultry("wldpoultryproduction"))*100)).select("year_col","wldpoultryproduction","comparison_poultry").orderBy("year_col")

    //rice Data
    val wldrice = spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("world_rice.txt")
    val usrice =  spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("usa_rice.txt")
    val df_wldrice = wldrice.as("wldric")
    val df_usrice = usrice.as("usric")
    val rice_join = df_wldrice.join(df_usrice, df_wldrice("year")===df_usrice("year"), "fullouter").select("wldric.year", "usric.harvest", "wldric.harvest").toDF("year_col","us_harvest","wld_harvest")
    val comprice = rice_join.withColumn("usriceharvest", rice_join("us_harvest").cast(DoubleType)).withColumn("wldriceharvest", rice_join("wld_harvest").cast(DoubleType)).select("year_col","usriceharvest","wldriceharvest")
    val riceData = comprice.withColumn("comparison_rice", toDouble((comprice("usriceharvest")/comprice("wldriceharvest"))*100)).select("year_col","wldriceharvest","comparison_rice").orderBy("year_col")

    //sorghum Data
    val wldsorghum = spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("world_sorghum.txt")
    val ussorghum =  spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("usa_sorghum.txt")
    val df_wldsorghum = wldsorghum.as("wldsorg")
    val df_ussorghum = ussorghum.as("ussorg")
    val sorghum_join = df_wldsorghum.join(df_ussorghum, df_wldsorghum("year")===df_ussorghum("year"), "fullouter").select("wldsorg.year", "ussorg.harvest", "wldsorg.harvest").toDF("year_col","us_harvest","wld_harvest")
    val compsorghum = sorghum_join.withColumn("ussorghumharvest", sorghum_join("us_harvest").cast(DoubleType)).withColumn("wldsorghumharvest", sorghum_join("wld_harvest").cast(DoubleType)).select("year_col","ussorghumharvest","wldsorghumharvest")
    val sorghumData = compsorghum.withColumn("comparison_sorghum", toDouble((compsorghum("ussorghumharvest")/compsorghum("wldsorghumharvest"))*100)).select("year_col","wldsorghumharvest","comparison_sorghum").orderBy("year_col")

    //soybeanmeal Data
    val wldsoybeanmeal = spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("world_soybeanmeal.txt")
    val ussoybeanmeal =  spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("usa_soybeanmeal.txt")
    val df_wldsoybeanmeal = wldsoybeanmeal.as("wldsbm")
    val df_ussoybeanmeal = ussoybeanmeal.as("ussbm")
    val soybeanmeal_join = df_wldsoybeanmeal.join(df_ussoybeanmeal, df_wldsoybeanmeal("year")===df_ussoybeanmeal("year"), "fullouter").select("wldsbm.year", "ussbm.production", "wldsbm.production").toDF("year_col","us_production","wld_production")
    val compsoybeanmeal = soybeanmeal_join.withColumn("ussoybeanmeal", soybeanmeal_join("us_production").cast(DoubleType)).withColumn("wldsoybeanmeal", soybeanmeal_join("wld_production").cast(DoubleType)).select("year_col","ussoybeanmeal","wldsoybeanmeal")
    val soybeanmealData = compsoybeanmeal.withColumn("comparison_soybeanmeal", toDouble((compsoybeanmeal("ussoybeanmeal")/compsoybeanmeal("wldsoybeanmeal"))*100)).select("year_col","wldsoybeanmeal","comparison_soybeanmeal").orderBy("year_col")

    //soybeanoil Data
    val wldsoybeanoil = spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("world_soybeanoil.txt")
    val ussoybeanoil =  spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("usa_soybeanoil.txt")
    val df_wldsoybeanoil = wldsoybeanoil.as("wldsbo")
    val df_ussoybeanoil = ussoybeanoil.as("ussbo")
    val soybeanoil_join = df_wldsoybeanoil.join(df_ussoybeanoil, df_wldsoybeanoil("year")===df_ussoybeanoil("year"), "fullouter").select("wldsbo.year", "ussbo.production", "wldsbo.production").toDF("year_col","us_production","wld_production")
    val compsoybeanoil = soybeanoil_join.withColumn("ussoybeanoil", soybeanoil_join("us_production").cast(DoubleType)).withColumn("wldsoybeanoil", soybeanoil_join("wld_production").cast(DoubleType)).select("year_col","ussoybeanoil","wldsoybeanoil")
    val soybeanoilData = compsoybeanoil.withColumn("comparison_soybeanoil", toDouble((compsoybeanoil("ussoybeanoil")/compsoybeanoil("wldsoybeanoil"))*100)).select("year_col","wldsoybeanoil","comparison_soybeanoil").orderBy("year_col")

    //soybeens Data
    val wldsoybeens = spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("world_soybeens.txt")
    val ussoybeens =  spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("usa_soybeens.txt")
    val df_wldsoybeens = wldsoybeens.as("wldsoy")
    val df_ussoybeens = ussoybeens.as("ussoy")
    val soybeens_join = df_wldsoybeens.join(df_ussoybeens, df_wldsoybeens("year")===df_ussoybeens("year"), "fullouter").select("wldsoy.year", "ussoy.harvest", "wldsoy.harvest").toDF("year_col","us_harvest","wld_harvest")
    val compsoybeens = soybeens_join.withColumn("ussoybeens", soybeens_join("us_harvest").cast(DoubleType)).withColumn("wldsoybeens", soybeens_join("wld_harvest").cast(DoubleType)).select("year_col","ussoybeens","wldsoybeens")
    val soybeensData = compsoybeens.withColumn("comparison_soybeens", toDouble((compsoybeens("ussoybeens")/compsoybeens("wldsoybeens"))*100)).select("year_col","wldsoybeens","comparison_soybeens").orderBy("year_col")

    //wheat Data
    val wldwheat = spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("world_wheat.txt")
    val uswheat =  spark.read.format("csv").option("delimter","\t").option("sep", "\t").option("header","true").load("usa_wheat.txt")
    val df_wldwheat = wldwheat.as("wldwht")
    val df_uswheat = uswheat.as("uswht")
    val wheat_join = df_wldwheat.join(df_uswheat, df_wldwheat("year")===df_uswheat("year"), "fullouter").select("wldwht.year", "uswht.harvest", "wldwht.harvest").toDF("year_col","us_harvest","wld_harvest")
    val compwheat = wheat_join.withColumn("uswheat", wheat_join("us_harvest").cast(DoubleType)).withColumn("wldwheat", wheat_join("wld_harvest").cast(DoubleType)).select("year_col","uswheat","wldwheat")
    val wheatData = compwheat.withColumn("comparison_wheat", toDouble((compwheat("uswheat")/compwheat("wldwheat"))*100)).select("year_col","wldwheat","comparison_wheat").orderBy("year_col")

    val products_data = barleyData.join(
      cornData,"year_col").join(
      cottonData,"year_col").join(
      riceData,"year_col").join(
      sorghumData,"year_col").join(
      soybeanmealData,"year_col").join(
      soybeanoilData,"year_col").join(
      soybeensData,"year_col").join(
      wheatData,"year_col")

//    products_data.show()
//
//    products_data.withColumn("year", col("year_col"))


//    val x = products_data.select("year_col").map(x => x.toString.split("/")).map(x => x(0)).map(x => x.split('[')).map(x => x(1)).toDF("year_col")
//    products_data.join(x, x("year_col")===products_data("year_col"), "fullouter").show()

   val prdData =  products_data.withColumn("year_val", substring($"year_col",1,4)).drop("year_col").withColumnRenamed("year_val","year_col")

    val meat_data = beefData.join(
      porkData, "year_col").join(
      poultryData, "year_col")

    val Data = prdData.join(meat_data, prdData("year_col") === meat_data("year_col"), "outer").withColumnRenamed("comparison_barley","usa_barley_contribution%")
      .withColumnRenamed("comparison_corn","usa_corn_contribution%")
      .withColumnRenamed("comparison_cotton","usa_cotton_contribution%")
      .withColumnRenamed("comparison_rice","usa_rice_contribution%")
      .withColumnRenamed("comparison_sorghum","usa_sorghum_contribution%")
      .withColumnRenamed("comparison_soybeanmeal","usa_soybeanmeal_contribution%")
      .withColumnRenamed("comparison_soybeanoil","usa_soybeanoil_contribution%")
      .withColumnRenamed("comparison_soybeens","usa_soybeens_contribution%")
      .withColumnRenamed("comparison_wheat","usa_wheat_contribution%")
      .withColumnRenamed("comparison_beef","usa_beefslaughter_contribution%")
      .withColumnRenamed("comparison_pork","usa_porkslaughter_contribution%")
      .withColumnRenamed("comparison_poultry","usa_poultryproduction_contribution%").drop(prdData("year_col"))

    val finalData = Data.select("year_col","wld_barleyharvest","usa_barley_contribution%",
           "wldbeefslaughter","usa_beefslaughter_contribution%",
          "wldcornharvest","usa_corn_contribution%",
      "wldcottonharvest","usa_cotton_contribution%",
    "wldporkslaughter","usa_porkslaughter_contribution%",
    "wldpoultryproduction","usa_poultryproduction_contribution%",
          "wldriceharvest","usa_rice_contribution%",
      "wldsorghumharvest","usa_sorghum_contribution%",
      "wldsoybeanmeal","usa_soybeanmeal_contribution%",
      "wldsoybeanoil","usa_soybeanoil_contribution%",
      "wldsoybeens","usa_soybeens_contribution%",
      "wldwheat","usa_wheat_contribution%"
    ).orderBy("year_col")

    finalData.write.format("csv").option("header","True").mode("overwrite").option("sep","|").save("finalData.csv")






  }

}
