package org.omics.mongoop

import com.mongodb.spark.MongoSpark
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{split, sum}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, functions}
import org.bson.Document
import org.omics.sparkop.SparkInfo
import org.omics.utils.Constants

import scala.collection.mutable


object SparkMongo {

  val sqlContext = SparkInfo.getSqlContext()

  //def denominatorDf = omicsConDenominators

  def getAggregateData :DataFrame = {

    val rdd = MongoSpark.load(SparkInfo.getSparkSession().sparkContext) // Uses the SparkSession
    //println(rdd.first.toJson)
    //'downloadCount':'$additional.download_count',

    val aggregatedRdd = rdd.withPipeline(Seq(
      //Document.parse("{ $match: { 'additional.search_domains' : { $exists :true } } }"),
      //Document.parse("{ $match: { 'database' : 'BioModels' } }"),
      //Document.parse("{ $match: { 'additional.download_count' : { $exists :true } } }"),
      /*commented this latest on 25 November 2020 Document.parse(
        "{$project : {'accession':1, 'database':1, 'omicsType':'$additional.omics_type'," +
          "'species':'$additional.species', 'tissue':'$additional.tissue', 'disease':'$additional.disease'," +
          "'searchDomain':'$additional.search_domains'," +
          "'ensembl':'$crossReferences.ensembl', 'uniprot': '$crossReferences.uniprot', " +
          "'citationCount':'$additional.citation_count', 'reanalysisCount':'$additional.reanalysis_count',"+
          "'viewCount':'$additional.view_count', 'downloadCount':'$additional.download_count'" +
          "'searchCount':'$additional.search_count'" +
          "'citationCountNormalized':'$additional.citation_count_scaled','reanalysisCountNormalized':'$additional.reanalysis_count_scaled',"+
          "'viewCountNormalized':'$additional.view_count_scaled','downloadCountNormalized':'$additional.download_count_scaled'," +
          "'searchCountNormalized':'$additional.normalized_connections'" +
          "}}")*/
/*      Document.parse(
        "{$project : {'accession':1, 'database':1, 'omicsType':'$additional.omics_type'," +
          "'species':'$additional.species','tissue':'$additional.tissue','disease':'$additional.disease'," +
          "'citationCount':'$additional.citation_count','reanalysisCount':'$additional.reanalysis_count',"+
          "'searchDomain':'$additional.search_domains','searchCount':'$additional.search_count'," +
          "'ensembl':'$crossReferences.ensembl','uniprot': '$crossReferences.uniprot','viewCount':'$additional.view_count'}}")*/
      Document.parse(
        "{$project : {'accession':1, 'database':1,'omics_type':'$additional.omics_type'"  +
          "'citationCount':'$additional.citation_count','reanalysisCount':'$additional.reanalysis_count',"+
          "'searchCount':'$additional.search_count'," +
          "'viewCount':'$additional.view_count','downloadCount':'$additional.download_count' " +
          "'citationCountNormalized':'$additional.citation_count_scaled','reanalysisCountNormalized':'$additional.reanalysis_count_scaled',"+
          "'viewCountNormalized':'$additional.view_count_scaled','downloadCountNormalized':'$additional.download_count_scaled'," +
          "'searchCountNormalized':'$additional.normalized_connections'" +
          "}}"),
      Document.parse("{$limit:500}")
    ))
    //println(aggregatedRdd.count)
    //aggregatedRdd.take(10).foreach(dt => println(dt.toJson))

    //println(aggregatedRdd.toDF.printSchema())

    //println("count of records is " + aggregatedRdd.count())

    aggregatedRdd.toDF()

  }

  def getProcesseData(aggregateDf:DataFrame) :DataFrame= {

    import sqlContext.implicits._

    val csvDf = aggregateDf

    //csvDf.take(10).foreach(dt => println(dt))
/*
    val explodeDF = csvDf.withColumn(Constants.flatDomain, functions.explode_outer($"searchDomain"))
      .withColumn(Constants.flatTissue, functions.explode_outer($"tissue"))
      .withColumn(Constants.flatDisease, functions.explode_outer($"disease"))
      .withColumn(Constants.flatSpecies, functions.explode_outer($"species"))
      .withColumn(Constants.flatOmicsType, functions.explode_outer($"omicsType"))
      .withColumn(Constants.flatViewCount, functions.explode_outer($"viewCount"))
      .withColumn(Constants.flatSearchCount, functions.explode_outer($"searchCount"))
      .withColumn(Constants.flatReanalysisCount, functions.explode_outer($"reanalysisCount"))
      .withColumn(Constants.flatCitationCount, functions.explode_outer($"citationCount"))
      //.withColumn(Constants.flatDownloadCount, functions.explode_outer($"downloadCount"))*/
    //


    /*commented latest on 2020*/
    /*val explodeDF = csvDf.withColumn(Constants.flatDomain, functions.explode_outer($"searchDomain"))
      .withColumn(Constants.flatTissue, functions.explode_outer($"tissue"))
      .withColumn(Constants.flatDisease, functions.explode_outer($"disease"))
      .withColumn(Constants.flatSpecies, functions.explode_outer($"species"))
      .withColumn(Constants.flatOmicsType, functions.explode_outer($"omicsType"))
      .withColumn(Constants.flatViewCount, functions.explode_outer($"viewCount"))
      .withColumn(Constants.flatSearchCount, functions.explode_outer($"searchCount"))
      .withColumn(Constants.flatReanalysisCount, functions.explode_outer($"reanalysisCount"))
      .withColumn(Constants.flatCitationCount, functions.explode_outer($"citationCount"))
      .withColumn(Constants.flatDownloadCount, functions.explode_outer($"downloadCount"))
      .withColumn(Constants.flatViewCountNormalized, functions.explode_outer($"viewCountNormalized"))
      .withColumn(Constants.flatSearchCountNormalized, functions.explode_outer($"searchCountNormalized"))
      .withColumn(Constants.flatReanalysisCountNormalized, functions.explode_outer($"reanalysisCountNormalized"))
      .withColumn(Constants.flatCitationCountNormalized, functions.explode_outer($"citationCountNormalized"))
      .withColumn(Constants.flatDownloadCountNormalized, functions.explode_outer($"downloadCountNormalized"))*/

    val explodeDF = csvDf
      .withColumn(Constants.flatViewCount, functions.explode_outer($"viewCount"))
      .withColumn(Constants.flatSearchCount, functions.explode_outer($"searchCount"))
      .withColumn(Constants.flatReanalysisCount, functions.explode_outer($"reanalysisCount"))
      .withColumn(Constants.flatCitationCount, functions.explode_outer($"citationCount"))
      .withColumn(Constants.flatDownloadCount, functions.explode_outer($"downloadCount"))
      .withColumn(Constants.flatViewCountNormalized, functions.explode_outer($"viewCountNormalized"))
      .withColumn(Constants.flatSearchCountNormalized, functions.explode_outer($"searchCountNormalized"))
      .withColumn(Constants.flatReanalysisCountNormalized, functions.explode_outer($"reanalysisCountNormalized"))
      .withColumn(Constants.flatCitationCountNormalized, functions.explode_outer($"citationCountNormalized"))
      .withColumn(Constants.flatDownloadCountNormalized, functions.explode_outer($"downloadCountNormalized"))

    //explodeDF.take(10).foreach(dt => println(dt))

    //println(explodeDF.count())
    //df.select($"_id", $"addresses"(0)("street"), $"country"("name"))

   /* commented latest on 25th November 2020
      val domainCount = explodeDF.withColumn("tempColumn", split(explodeDF.col(Constants.flatDomain), "~")).
      //withColumn($"tempColumn".getItem(0).toString() , $"tempColumn".getItem(1))
      withColumn(Constants.finalDomain, $"tempColumn".getItem(0))
      .withColumn(Constants.domainCount, $"tempColumn".getItem(1))

    val finalDF = domainCount.drop("tempColumn", Constants.flatDomain, "searchDomain")*/

    //finalDF.printSchema()

    ///println("count of records is " + explodeDF.count())

    explodeDF
  }

  def saveDataToFile(filePath:String, finalDF:DataFrame) {
    //finalDF.take(10).foreach(dt => println(dt))

    import sqlContext.implicits._

    val list = List("accession", "uniprot", "sgd", "ncbi", "ensembl", "coding", "ec-code", "rnacentral",
      "sra", "go", "intact", "reactome", "kegg.compound", "kegg.genes", "kegg.glycan", "kegg.pathway",
      "kegg.reaction", "citations", "database", "disease", "metabolights", "omics_type", "reanalysis",
      "species", "tissue", "pubchem.compound", "pubchem.substance")


    val filteredDomains = finalDF//.filter($"finalDomain".isin(list:_*) )

    val pivotDf = filteredDomains
      .groupBy(Constants.accession,Constants.datasetDatabase,Constants.flatOmicsType,
        Constants.flatViewCount,Constants.flatSearchCount,Constants.flatReanalysisCount,Constants.flatCitationCount,
        Constants.flatTissue,Constants.flatDisease,Constants.flatSpecies,Constants.flatDownloadCount,
        Constants.flatViewCountNormalized,Constants.flatSearchCountNormalized,Constants.flatReanalysisCountNormalized,
        Constants.flatCitationCountNormalized, Constants.flatDownloadCountNormalized
      )
        .pivot(Constants.finalDomain).agg(sum($"domainCount"))

    //pivotDf.take(10).foreach(dt => println(dt))
    pivotDf.printSchema()

    println("count of rows is " + pivotDf.count())

    pivotDf.coalesce(1).write
      .format(Constants.sparkWriteFormat)
      .option("header", "true")
      .mode(SaveMode.Overwrite).save(Constants.savePath)
  }

  def normalizeMetrics(inputDf:DataFrame, omicsDf:mutable.HashMap[String,Double]) {

    MongoUpdates.getCitationMaxMinValue()
    MongoUpdates.getReanalysisMaxMinValue()
    MongoUpdates.getViewMaxMinValue()
    MongoUpdates.getDownloadMaxMinValue()
    //val maxminMap = scala.collection.mutable.HashMap.empty[String,Double]
    val maxminMap = Map(Constants.maxViewCount->MongoUpdates.objList.maxViewCount,
      Constants.minViewCount-> MongoUpdates.objList.minViewCount,
      Constants.maxReanalysisCount->MongoUpdates.objList.maxReanalysisCount,
      Constants.minReanalysisCount-> MongoUpdates.objList.minReanalysisCount,
      Constants.maxCitationCount->MongoUpdates.objList.maxCitationCount,
      Constants.minCitationCount-> MongoUpdates.objList.minCitationCount,
      Constants.maxDownloadCount->MongoUpdates.objList.maxDownloadCount,
      Constants.minDownloadCount-> MongoUpdates.objList.minDownloadCount
    )
    println(maxminMap.formatted("-"))

    //MongoUpdates.getMaxFieldValue()
    //print(inputDf.count())
    //MongoUpdates.getSearchMaxMinValue()
    /*MongoUpdates.getCitationMaxMinValue()
    MongoUpdates.getReanalysisMaxMinValue()
    MongoUpdates.getViewMaxMinValue()
    MongoUpdates.getDownloadMaxMinValue()*/
    MongoUpdates.objList
    inputDf.rdd.map(dt => {
      MongoUpdates.normalize(dt, omicsDf, maxminMap);
    }).count()
    //MongoUpdates.normalize()

  }

  def omicsConDenominators(): DataFrame ={
    import sqlContext.implicits._
    val omicsDf = SparkInfo.getSqlContext().read.
      format("csv").option("header", "true")
     .load("/user/gdass/connections.csv")
    //.load("/home/gaur/connections.csv")
    //"file:///homes/gdass/connections.csv"
    //omicsDf.show()
    omicsDf.toDF()
  }

   /*def main(args: Array[String]): Unit = {

      //SparkMongo.omicsConDenominators(SparkMongo.sqlContext)
      //SparkMongo.getAggregateData.show()
      val map = scala.collection.mutable.HashMap.empty[String,Double]
      val omicsCount = SparkMongo.omicsConDenominators.collect.foreach(r => map += (r.get(0).toString -> r.get(1).toString.toInt))
      //map
      //MongoUpdates.getCitationMaxMinValue()
      SparkMongo.normalizeMetrics(SparkMongo.getProcesseData(SparkMongo.getAggregateData), map)
    }*/

}

