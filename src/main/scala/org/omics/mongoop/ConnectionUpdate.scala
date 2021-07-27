package org.omics.mongoop

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}
import org.omics.model.Dataset
import org.omics.sparkop.SparkInfo
import org.omics.utils.Constants


object ConnectionUpdate {

  val schema = new StructType()
    .add(Constants.accession,StringType,true)
    .add(Constants.connectionsColumn,DoubleType,true)
    .add(Constants.datasetDatabase,StringType,true)

  def updateConnections():Unit= {

    val fileDf = SparkInfo.readCsv(Constants.connectionFilePath, SparkInfo.getSqlContext())
    fileDf.printSchema()

    val requiredCol = fileDf.select(Constants.accession,Constants.datasetDatabase,Constants.connectionsColumn)
    requiredCol.foreach(dt => updateConnectionCount(dt))
  }

  def updateConnectionCount(input:Row):Unit ={
    print(input)
    //MongoUpdates.updateCasbahImports(new Dataset(input.get(0).toString, input.getString(1), (input.get(2).asInstanceOf[Double]/5).toString,))
  }


}
