package org.omics.mongoop

import org.apache.spark.sql.{DataFrame, Row}
import org.omics.mongoop.SparkMongo.sqlContext
import org.omics.sparkop.SparkInfo

import scala.collection.mutable

object OmicsUpdateVocab {

  def readOmicsVocab(): DataFrame ={
    import sqlContext.implicits._
    val omicsDf = SparkInfo.getSqlContext().read.
      format("csv").option("header", "true")
      //.load("/user/gdass/connections.csv")
      //.load("/home/gaur/Downloads/omicstypevocab.csv")
      .load("/home/gaur/Downloads/omicsdivocab.csv")
    //"file:///homes/gdass/connections.csv"
    //omicsDf.show()
    omicsDf.show()

    omicsDf
  }

  def updateOmics(currOmics: String, vocabMap:mutable.HashMap[String, String], row:Row): String ={
    //val vocabMap = OmicsUpdateVocab.readOmicsVocab()
    val omicsType = "test"
    //val currOmics = "Poly-ADP-ribosylation by proteomics"
    val currOmicsList = row.getAs[mutable.WrappedArray[String]]("omics_type")
    val currOmics = if ( currOmicsList !=null &&  currOmicsList.length > 0) currOmicsList.head else
      {
        println("accession of empty omics type is" +  row.getAs[String]("accession"))
        "empty" }
    //val updatedOmics = "updated"
    import SparkMongo.sqlContext.implicits._
    val updatedOmics =  vocabMap.map(r => {
      val ismatched = currOmics.matches("(?i).*" +r._1 + ".*")
      if (ismatched.equals(true))
          r._2
      else
        currOmics
    }).head
    //"Population Genomics".matches("(?i)^Genomics|.*\\sGenomics")
    println("current omics is " , currOmics, " updated omics is ", updatedOmics)
    updatedOmics

  }

  def main(args:Array[String]): Unit ={
      val vocabMap = mutable.HashMap.empty[String,String]
      import sqlContext.implicits._
      val omicsVocab = OmicsUpdateVocab.readOmicsVocab().collect.foreach(r => vocabMap += (r.get(0).toString -> r.get(2).toString))
      SparkMongo.getProcesseData(SparkMongo.getAggregateData).map(r => updateOmics("test", vocabMap, r)).count()
  }
}
