package com.josedeveloper

//import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

case class Persona(ID: Int, NOMBRE: String, APELLIDO: String, DNI: String, CIUDAD: String, PAIS: String, EDAD: Int, PESO: Int, ALTURA: Double)

object DataFrameFromCSVApp extends App {

  // En spark v1.6.2 <= lo hubieramos hecho asi
  //  val conf = new SparkConf().setMaster("local").setAppName("Simple Application")
  //  val sc = new SparkContext(conf)
  //  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("Simple Application")
    .getOrCreate()

  val df = sparkSession.read.format("com.databricks.spark.csv")
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("src/main/resources/datos.csv")

  df.show()

  df.printSchema()

  df.describe().show()

  df.describe("EDAD","PESO").show()

  df.filter("EDAD < 30").show()
}
