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

  val downloadmaxaccum = sqlContext.sparkContext.doubleAccumulator("downloadmax")

  val viewmaxaccum = sqlContext.sparkContext.doubleAccumulator("viewmax")

  val citationmaxaccum = sqlContext.sparkContext.doubleAccumulator("citationmax")

  val reanalysismaxaccum = sqlContext.sparkContext.doubleAccumulator("reanalysismax")

  //def denominatorDf = omicsConDenominators

  def getAggregateData :DataFrame = {

    val rdd = MongoSpark.load(SparkInfo.getSparkSession().sparkContext) // Uses the SparkSession
    //println(rdd.first.toJson)
    //'downloadCount':'$additional.download_count',

    val aggregatedRdd = rdd.withPipeline(Seq(
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
      .withColumn(Constants.flatOmicsType, functions.explode_outer($"omics_type"))

    explodeDF
  }

  def saveDataToFile(filePath:String, finalDF:DataFrame) {

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

  def normalizeMetrics(inputDf:DataFrame, omicsDf:mutable.HashMap[String,Double], vocabMap:mutable.HashMap[String, String]) {

/*    MongoUpdates.getCitationMaxMinValue()
    MongoUpdates.getReanalysisMaxMinValue()
    MongoUpdates.getViewMaxMinValue()
    MongoUpdates.getDownloadMaxMinValue()*/

    inputDf.rdd.map(dt => {
      MongoUpdates.normalize(dt, omicsDf, vocabMap);
    }).count()

    //MongoUpdates.normalize()
    //println("accumulator values are " , SparkMongo.downloadmaxaccum.value, SparkMongo.citationmaxaccum, SparkMongo.viewmaxaccum, SparkMongo.reanalysismaxaccum)

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

