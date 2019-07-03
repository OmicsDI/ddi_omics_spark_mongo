package org.omics.mongoop

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.functions.{split, sum}
import org.apache.spark.sql.{DataFrame, SaveMode, functions}
import org.bson.Document
import org.omics.sparkop.SparkInfo
import org.omics.utils.Constants


object SparkMongo {

  val sqlContext = SparkInfo.getSqlContext()

  def getAggregateData :DataFrame = {

    val rdd = MongoSpark.load(SparkInfo.getSparkSession().sparkContext) // Uses the SparkSession
    //println(rdd.first.toJson)
    //'downloadCount':'$additional.download_count',

    val aggregatedRdd = rdd.withPipeline(Seq(
      //Document.parse("{ $match: { 'additional.search_domains' : { $exists :true } } }"),
      //Document.parse("{ $match: { 'database' : 'BioModels' } }"),
      //Document.parse("{ $match: { 'additional.download_count' : { $exists :true } } }"),
      Document.parse(
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
          "}}")
/*      Document.parse(
        "{$project : {'accession':1, 'database':1, 'omicsType':'$additional.omics_type'," +
          "'species':'$additional.species','tissue':'$additional.tissue','disease':'$additional.disease'," +
          "'citationCount':'$additional.citation_count','reanalysisCount':'$additional.reanalysis_count',"+
          "'searchDomain':'$additional.search_domains','searchCount':'$additional.search_count'," +
          "'ensembl':'$crossReferences.ensembl','uniprot': '$crossReferences.uniprot','viewCount':'$additional.view_count'}}")*/
    ))
    //println(aggregatedRdd.count)
    //aggregatedRdd.take(10).foreach(dt => println(dt.toJson))

    println(aggregatedRdd.toDF.printSchema())



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

    val explodeDF = csvDf.withColumn(Constants.flatDomain, functions.explode_outer($"searchDomain"))
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
      .withColumn(Constants.flatDownloadCountNormalized, functions.explode_outer($"downloadCountNormalized"))

    explodeDF.take(10).foreach(dt => println(dt))

    //println(explodeDF.count())
    //df.select($"_id", $"addresses"(0)("street"), $"country"("name"))

    val domainCount = explodeDF.withColumn("tempColumn", split(explodeDF.col(Constants.flatDomain), "~")).
      //withColumn($"tempColumn".getItem(0).toString() , $"tempColumn".getItem(1))
      withColumn(Constants.finalDomain, $"tempColumn".getItem(0))
      .withColumn(Constants.domainCount, $"tempColumn".getItem(1))

    val finalDF = domainCount.drop("tempColumn", Constants.flatDomain, "searchDomain")

    finalDF.printSchema()

    finalDF
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

    pivotDf.take(10).foreach(dt => println(dt))
    pivotDf.printSchema()

    println("count of rows is " + pivotDf.count())

    pivotDf.coalesce(1).write
      .format(Constants.sparkWriteFormat)
      .option("header", "true")
      .mode(SaveMode.Overwrite).save(Constants.savePath)
  }

  def normalizeMetrics(inputDf:DataFrame) {

    //MongoUpdates.getMaxFieldValue()
    //print(inputDf.count())
    //MongoUpdates.getSearchMaxMinValue()
    MongoUpdates.getCitationMaxMinValue()
    MongoUpdates.getReanalysisMaxMinValue()
    MongoUpdates.getViewMaxMinValue()
    MongoUpdates.getDownloadMaxMinValue()
    MongoUpdates.objList
    inputDf.rdd.map(dt => {
      MongoUpdates.normalize(dt);
    }).count()
    //MongoUpdates.normalize()

  }

}
