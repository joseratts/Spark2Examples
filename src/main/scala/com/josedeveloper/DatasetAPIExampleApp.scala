package com.josedeveloper

//import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

case class People(firstName: String, secondName: String, lastName: String, age: Long)

object DatasetAPIExampleApp extends App {

  // En spark v1.6.2 <= lo hubieramos hecho asi
  //  val conf = new SparkConf().setMaster("local").setAppName("Simple Application")
  //  val sc = new SparkContext(conf)
  //  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val sparkSession = SparkSession.builder.
    master("local")
    .appName("Simple Application")
    .getOrCreate()

  //import sqlContext.implicits._
  import sparkSession.implicits._

  //obtenemos el dataset
  val ds = sparkSession.read.json("src/main/resources/people.json").as[People]

  ds.show()

  ds.printSchema()

  ds.filter(p => p.age <= 30).map(p => new People(p.firstName.toUpperCase, p.secondName, p.lastName, p.age)).show()

}
