package com.josedeveloper

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SimpleStatisticsWithSparkV2 extends App {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("Simple Application")
    .getOrCreate()

  //custom scheme definition
  val customSchema = StructType(Array(
    StructField("COD_DISTRITO", StringType, true),
    StructField("DESC_DISTRITO", StringType, true),
    StructField("COD_DIST_BARRIO", StringType, true),
    StructField("DESC_BARRIO", StringType, true),
    StructField("COD_BARRIO", StringType, true),
    StructField("COD_DIST_SECCION", StringType, true),
    StructField("COD_SECCION", StringType, true),
    StructField("COD_EDAD_INT", StringType, true),
    StructField("EspanolesHombres", IntegerType, true),
    StructField("EspanolesMujeres", IntegerType, true),
    StructField("ExtranjerosHombres", IntegerType, true),
    StructField("ExtranjerosMujeres", IntegerType, true)))

  val df = sparkSession.read.format("com.databricks.spark.csv")
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter" , ";")
    .option("nullValue", null)
    .schema(customSchema)
    .load("src/main/resources/Rango_Edades_Seccion_201506.csv")

  df.show(true)

  //fill empty values with 0
  val dfNA = df.na.fill(0, Seq("EspanolesHombres","EspanolesMujeres","ExtranjerosHombres","ExtranjerosMujeres")).cache()
  dfNA.show(true)

  dfNA.groupBy("DESC_DISTRITO")
    .agg(avg("EspanolesHombres"), avg("EspanolesMujeres"), avg("ExtranjerosHombres"), avg("ExtranjerosMujeres"))
    .show()

  dfNA.groupBy("DESC_DISTRITO")
    .agg(max("EspanolesHombres"), stddev("EspanolesHombres"), max("EspanolesMujeres"), stddev("EspanolesMujeres"))
    .show()

  val dfNASum = dfNA.groupBy(df("DESC_DISTRITO"))
    .sum("EspanolesHombres","EspanolesMujeres","ExtranjerosHombres","ExtranjerosMujeres").cache()

  //Total of people per district
  dfNASum.select(dfNASum("DESC_DISTRITO"), (dfNASum("sum(EspanolesHombres)") + dfNASum("sum(EspanolesMujeres)") + dfNASum("sum(ExtranjerosHombres)") + dfNASum("sum(ExtranjerosMujeres)")).alias("total"))
    .sort(asc("total"))
    .show(30)
}