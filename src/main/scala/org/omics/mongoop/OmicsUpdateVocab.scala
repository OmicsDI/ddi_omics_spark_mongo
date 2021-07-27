package org.omics.mongoop

import org.apache.spark.sql.{DataFrame, Row}
import org.omics.mongoop.SparkMongo.sqlContext
import org.omics.sparkop.SparkInfo

import java.util
import scala.collection.mutable

object OmicsUpdateVocab {

  def readOmicsVocab(): DataFrame ={
    val omicsDf = SparkInfo.getSqlContext().read.
      format("csv").option("header", "true")
      //.load("/home/gaur/Downloads/omicsdivocab.csv")
      .load("/user/gdass/omicsdivocab.csv")
    omicsDf.show()

    omicsDf
  }

  def updateOmics(vocabMap:mutable.HashMap[String, String], row:Row): String ={
    val currOmicsList = row.getAs[mutable.WrappedArray[String]]("omics_type")
    val currOmics = if ( currOmicsList !=null &&  currOmicsList.length > 0) currOmicsList.head else
      {
        println("accession of empty omics type is" +  row.getAs[String]("accession"))
        "empty" }
    import SparkMongo.sqlContext.implicits._
    val updatedOmics =  vocabMap.map(r => {
      val ismatched = currOmics.matches("(?i).*" +r._1 + ".*")
      if (ismatched.equals(true))
          r._2
      else
        currOmics
    }).head
    println("current omics is " , currOmics, " updated omics is ", updatedOmics)
    updatedOmics

  }

  def getOmicsCVMap(): mutable.HashMap[String, String] ={
    val vocabMap = mutable.HashMap.empty[String,String]
    val omicsVocab = OmicsUpdateVocab.readOmicsVocab().collect.foreach(r => vocabMap += (r.get(0).toString -> r.get(2).toString))
    vocabMap
  }

  def getOmicsCVMap(vocabDf:DataFrame): mutable.HashMap[String, String] ={
    val vocabMap = mutable.HashMap.empty[String,String]
    val omicsVocab = vocabDf.collect.foreach(r => vocabMap += (r.get(0).toString -> r.get(2).toString))
    vocabMap
  }
/*  def main(args:Array[String]): Unit ={
      import sqlContext.implicits._
      SparkMongo.getProcesseData(SparkMongo.getAggregateData).map(r => updateOmics(getOmicsCVMap, r)).count()
  }*/
}
