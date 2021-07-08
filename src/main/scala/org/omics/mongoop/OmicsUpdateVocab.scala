package org.omics.mongoop

import org.apache.spark.sql.{DataFrame, Row}
import org.omics.mongoop.SparkMongo.sqlContext
import org.omics.sparkop.SparkInfo

object OmicsUpdateVocab {

  def readOmicsVocab(): Unit ={
    import sqlContext.implicits._
    val omicsDf = SparkInfo.getSqlContext().read.
      format("csv").option("header", "true")
      .load("/user/gdass/connections.csv")
      //.load("/home/gaur/Downloads/omicstypevocab.csv")
    //"file:///homes/gdass/connections.csv"
    //omicsDf.show()
    omicsDf.show()

  }

  def updateOmics(currOmics: String, vocabMap:DataFrame, row:Row): Unit ={

  }

  def main(args:Array[String]): Unit ={
      OmicsUpdateVocab.readOmicsVocab()
  }
}
