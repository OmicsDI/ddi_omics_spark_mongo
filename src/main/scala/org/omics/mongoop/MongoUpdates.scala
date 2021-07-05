package org.omics.mongoop

import com.mongodb.casbah.Imports.{$set, BasicDBList, BasicDBObject, MongoClient, MongoClientURI, MongoDBList, MongoDBObject}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.omics.model.{Dataset, MaxMinValues}
import org.omics.utils.Constants

import scala.collection.mutable
import scala.compat.java8.StreamConverters._


object MongoUpdates {

  val mongoClientURI = MongoClientURI(Constants.prodMongoUri)

  val mongoClient =  MongoClient.apply(mongoClientURI)

  //val objList = getMaxFieldValue().asInstanceOf[BasicDBList]

  val objList = new MaxMinValues

  val db = mongoClient(Constants.mongoDatabase)

  val coll = db(Constants.mongoCollection)


  def updateCasbahImports(dataset:Dataset):Unit ={

    val query = MongoDBObject(Constants.accession -> dataset.accession, Constants.datasetDatabase -> dataset.database)
    val update = $set(Constants.additionalSearchScaled -> Set(dataset.connections))
    val result = coll.update( query, update )

    println("Number updated: " + result.getN)

    }

  def getSearchMaxMinValue() = {

    val searchAggregateBuilder = MongoDBList.newBuilder

    searchAggregateBuilder += MongoDBObject(Constants.matchOperator -> MongoDBObject(
          "scores.searchCount" -> MongoDBObject("$nin" -> List(Double.NaN))
      ))
    searchAggregateBuilder += MongoDBObject(Constants.groupOperator -> MongoDBObject(
          Constants.id -> "null", // constant, so we'll just create one bucket
          Constants.maxSearchCount -> MongoDBObject(Constants.maxOperator -> Constants.searchScoresCol),
          Constants.minSearchCount -> MongoDBObject(Constants.minOperator -> Constants.searchScoresCol)
      ))

    val pipeline = searchAggregateBuilder.result()

    println(pipeline)

    val aggResult = aggregationResult(Constants.mongoCollection, pipeline)

    toList(aggResult)
  }

  def getCitationMaxMinValue() = {

    val citationAggBuilder = MongoDBList.newBuilder

    citationAggBuilder += MongoDBObject(Constants.matchOperator -> MongoDBObject(
      "scores.citationCount" -> MongoDBObject("$nin" -> List(Double.NaN))
    ))
    citationAggBuilder += MongoDBObject(Constants.groupOperator -> MongoDBObject(
      Constants.id -> "null", // constant, so we'll just create one bucket
      Constants.maxCitationCount -> MongoDBObject(Constants.maxOperator -> Constants.citationScoresCol),
      Constants.minCitationCount -> MongoDBObject(Constants.minOperator -> Constants.citationScoresCol)

    ))

    val pipeline = citationAggBuilder.result()

    println(pipeline)

    val aggResult = aggregationResult(Constants.mongoCollection, pipeline)
    toList(aggResult)
  }

  def getViewMaxMinValue() = {

    val viewAggBuilder = MongoDBList.newBuilder

    viewAggBuilder += MongoDBObject(Constants.matchOperator -> MongoDBObject(
      "scores.viewCount" -> MongoDBObject("$nin" -> List(Double.NaN))
    ))
    viewAggBuilder += MongoDBObject(Constants.groupOperator -> MongoDBObject(
      Constants.id -> "null", // constant, so we'll just create one bucket
      Constants.maxViewCount -> MongoDBObject(Constants.maxOperator -> Constants.viewScoresCol),
      Constants.minViewCount -> MongoDBObject(Constants.minOperator -> Constants.viewScoresCol)
    ))

    val pipeline = viewAggBuilder.result()

    println(pipeline)

    val aggResult = aggregationResult(Constants.mongoCollection, pipeline)

    toList(aggResult)
  }

  def getReanalysisMaxMinValue() = {

    val reanalysisAggBuilder = MongoDBList.newBuilder

    reanalysisAggBuilder += MongoDBObject(Constants.matchOperator -> MongoDBObject(
      Constants.reanalaysisScoresCol.replace("$","") -> MongoDBObject("$nin" -> List(Double.NaN))

    ))
    reanalysisAggBuilder += MongoDBObject(Constants.groupOperator -> MongoDBObject(
      Constants.id -> "null", // constant, so we'll just create one bucket
      Constants.maxReanalysisCount -> MongoDBObject(Constants.maxOperator -> Constants.reanalaysisScoresCol),
      Constants.minReanalysisCount -> MongoDBObject(Constants.minOperator -> Constants.reanalaysisScoresCol)))


    val pipeline = reanalysisAggBuilder.result()

    println(pipeline)

    val aggResult = aggregationResult(Constants.mongoCollection, pipeline)

    toList(aggResult)
  }

  def getDownloadMaxMinValue() = {

    val downloadAggBuilder = MongoDBList.newBuilder

    downloadAggBuilder += MongoDBObject(Constants.matchOperator -> MongoDBObject(
      Constants.downloadScoresCol.replace("$","") -> MongoDBObject("$exists" -> true)
    ))
    downloadAggBuilder += MongoDBObject(Constants.groupOperator -> MongoDBObject(
      Constants.id -> "null", // constant, so we'll just create one bucket
      Constants.maxDownloadCount -> MongoDBObject(Constants.maxOperator -> Constants.downloadScoresCol),
      Constants.minDownloadCount -> MongoDBObject(Constants.minOperator -> Constants.downloadScoresCol)
    ))

    val pipeline = downloadAggBuilder.result()

    println(pipeline)

    val aggResult = aggregationResult(Constants.mongoCollection, pipeline)

    toList(aggResult)
  }

  def aggregationResult(collectionName:String, pipeline: MongoDBList) = {
    val db = mongoClient(Constants.mongoDatabase)
    val cursor = db.command(MongoDBObject(Constants.aggregate -> collectionName,
      Constants.pipeline -> pipeline,"explain"-> false)).get("cursor").asInstanceOf[BasicDBObject]
    cursor.get("firstBatch")
  }

  def normalize(row:Row, omicsDF:mutable.HashMap[String,Double])  = {
      val maxCitationCount = objList.maxCitationCount
      val minCitationCount = objList.minCitationCount
      val maxSearchCount = objList.maxSearchCount
      val minSearchCount = objList.minSearchCount
      val maxReanalysisCount = objList.maxReanalysisCount
      val minReanalysisCount = objList.minReanalysisCount
      val maxViewCount = objList.maxViewCount
      val minViewCount = objList.minViewCount
      val maxDownloadCount = objList.maxDownloadCount
      val minDownloadCount = 0.0 //objList.minDownloadCount

      //print("maximum value of downloads is " + objList.maxDownloadCount)
      //print("minimum value of downloads is " + objList.minDownloadCount)

      /*val minCitationCount = toInt(aggregateData.getString(Constants.minCitationCount))
      val maxSearchCount = toInt(aggregateData.getString(Constants.maxSearchCount))
      val minSearchCount = toInt(aggregateData.getString(Constants.minSearchCount))
      val maxReanalysisCount = toInt(aggregateData.getString(Constants.maxReanalysisCount))
      val minReanalysisCount = toInt(aggregateData.getString(Constants.minReanalysisCount))
      val maxViewCount = toInt(aggregateData.getString(Constants.maxViewCount))
      val minViewCount = toInt(aggregateData.getString(Constants.minViewCount))*/

      val citationCountScaled = scaleFormula(maxCitationCount.toDouble, minCitationCount.toDouble, toInt(row.getAs(Constants.flatCitationCount)).toDouble)
      var reanalysisCountScaled = 0.0
      //if(row.getValuesMap(Seq(Constants.flatReanalysisCount)).get(Constants.flatReanalysisCount).get != null) {
        reanalysisCountScaled = scaleFormula(maxReanalysisCount.toDouble, minReanalysisCount.toDouble, toInt(row.getAs(Constants.flatReanalysisCount)).toDouble)
      //}
      val searchCountScaled = scaleConnections(row, omicsDF)
      val viewCountScaled = scaleFormula(maxViewCount.toDouble, minViewCount.toDouble, toInt(row.getAs(Constants.flatViewCount)).toDouble)
      var downloadCountScaled = if (row.getValuesMap(Seq(Constants.flatDownloadCount)).get(Constants.flatDownloadCount).get != null)  scaleFormula(maxDownloadCount.toDouble, minDownloadCount.toDouble, row.getAs(Constants.flatDownloadCount).toString.toDouble) else 0.0

    //row.getValuesMap(Seq(Constants.flatDownloadCount)).get(Constants.flatDownloadCount).get == null


      val accession = row.getAs[String](Constants.accession)
      val database = row.getAs[String](Constants.datasetDatabase)

      updateAllMetricsDataset(
        Dataset(accession, database, searchCountScaled.toString,
          reanalysisCountScaled.toString, viewCountScaled.toString,
          citationCountScaled.toString,downloadCountScaled.toString)
      )



  }

  def toInt(s: String): Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => 0
    }
  }

  def toList(dbObj: AnyRef) = {
    println(dbObj)
    dbObj match {
        case dblist:BasicDBList => dblist.stream.toScala[Stream].map(_ match {
          case o: BasicDBObject => {
            if (objList.maxCitationCount.equals(0) && o.containsField(Constants.maxCitationCount)) objList.maxCitationCount = o.get(Constants.maxCitationCount).asInstanceOf[Int]
            if (objList.minCitationCount.equals(0) && o.containsField(Constants.minCitationCount)) objList.minCitationCount = o.get(Constants.minCitationCount).asInstanceOf[Int]
            if (objList.maxSearchCount.equals(0) && o.containsField(Constants.maxSearchCount)) objList.maxSearchCount = o.get(Constants.maxSearchCount).asInstanceOf[Int]
            if (objList.minSearchCount.equals(0) && o.containsField(Constants.minSearchCount)) objList.minSearchCount = o.get(Constants.minSearchCount).asInstanceOf[Int]
            if (objList.maxReanalysisCount.equals(0) && o.containsField(Constants.maxReanalysisCount)) objList.maxReanalysisCount = toInt(o.get(Constants.maxReanalysisCount).toString.replace(".0",""))
            if (objList.minReanalysisCount.equals(0) && o.containsField(Constants.minReanalysisCount)) objList.minReanalysisCount = toInt(o.get(Constants.minReanalysisCount).toString.replace(".0",""))
            if (objList.maxViewCount.equals(0) && o.containsField(Constants.maxViewCount)) objList.maxViewCount = toInt(o.get(Constants.maxViewCount).toString.replace(".0",""))
            if (objList.minViewCount.equals(0) && o.containsField(Constants.minViewCount)) objList.minViewCount = toInt(o.get(Constants.minViewCount).toString.replace(".0",""))
//            if (objList.maxViewCount.equals(0) && o.containsField(Constants.maxViewCount)) objList.maxViewCount = o.get(Constants.maxViewCount).asInstanceOf[Int]
//            if (objList.minViewCount.equals(0) && o.containsField(Constants.minViewCount)) objList.minViewCount = o.get(Constants.minViewCount).asInstanceOf[Int]
            if (objList.maxDownloadCount.equals(0) && o.containsField(Constants.maxDownloadCount)) objList.maxDownloadCount = toInt(o.get(Constants.maxDownloadCount).toString.replace(".0",""))
            if (objList.minDownloadCount.equals(0) && o.containsField(Constants.minDownloadCount)) objList.minDownloadCount = toInt(o.get(Constants.minDownloadCount).toString.toString.replace(".0",""))
          }
      })
    }
  }

  def scaleFormula(max:Double, min:Double, currentValue:Double) :Double ={
        /*println("max value is " + max)
        println("min value is " + min)
        println("current value is" + currentValue)*/
        val normalizedValue = (currentValue - min) / (max - min)
        println(normalizedValue)
        normalizedValue
  }

  /*def scaleFormula(max:Double, min:Double, currentValue:Double) :Double ={
    println("max value is " + max)
    println("min value is " + min)
    println("current value is" + currentValue)
    val normalizedValue = (currentValue - min) / (max - min)
    println(normalizedValue)
    normalizedValue
  }*/

  def scaleConnections(row:Row, omicsDf:mutable.HashMap[String,Double]) :Double ={
    var normalizedValue = 0.0000

    if ( row.getAs(Constants.flatSearchCount) != null) {
      val searchCount = row.getAs(Constants.flatSearchCount).toString.toDouble
      val omicsType = if (row.getAs(Constants.omics_type) != null) {
        val arr = row.getAs(Constants.omics_type).asInstanceOf[mutable.WrappedArray[String]].array
        if (arr.length > 0) arr.head else "Unknown"
      } else ""
      //println("current value is" + searchCount)
      omicsDf
      val data = omicsDf.getOrElse(omicsType, 0.000).toDouble
      //val data = toInt(denominatorDF.filter(col("OmicsType").isin(omicsType)).first().get(1).toString)

      normalizedValue = if (searchCount >= data) 1.00 else searchCount/data
      //println("search value and normalized value is ", row.getAs(Constants.accession) , row.getAs(Constants.omics_type), normalizedValue, searchCount)
    }
    normalizedValue
  }

  def updateAllMetricsDataset(dataset:Dataset ):Unit ={
    println(dataset)
    val query = MongoDBObject(Constants.accession -> dataset.accession, Constants.datasetDatabase -> dataset.database)
    val update = $set(
      Constants.additionalSearchScaled -> Set(dataset.connections),
      Constants.additionalCitationScaled -> Set(dataset.citation) ,
      Constants.additionalReanalaysisScaled -> Set(dataset.reanalysis),
      Constants.additionalViewScaled -> Set(dataset.view),
      Constants.additionalDownloadScaled -> Set(dataset.download)
    )
    val result = coll.update( query, update )

    //println("Number updated: and accession updated is " + result.getN + " " +
      dataset.accession + "with search value " + dataset.connections

    println("accession updated is " + " " +
      dataset.accession + "with search value " + dataset.connections )
  }



}
