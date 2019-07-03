package org.omics.utils

object Constants {

  val config = ConfigData.getConfig()

  val accession = "accession"
  val datasetDatabase = "database"
  val scoreSearchCount = "scores.searchCount"
  val additionalSearchCount = "additional.search_count"
  val additionalDownloadCount = "additional.download_count"

  val additionalSearchScaled = "additional.normalized_connections"
  val additionalCitationScaled = "additional.citation_count_scaled"
  val additionalReanalaysisScaled = "additional.reanalysis_count_scaled"
  val additionalViewScaled = "additional.view_count_scaled"
  val additionalDownloadScaled = "additional.download_count_scaled"


  val connectionsColumn = "ajs_connectivity_score"

  val maxSearchCount = "maxSearchCount"
  val minSearchCount = "minSearchCount"
  val maxReanalysisCount = "maxReanalysisCount"
  val minReanalysisCount = "minReanalysisCount"
  val maxCitationCount = "maxCitationCount"
  val minCitationCount = "minCitationCount"
  val maxViewCount = "maxViewCount"
  val minViewCount = "minViewCount"
  val maxDownloadCount = "maxDownloadCount"
  val minDownloadCount = "minDownloadCount"


  val flatDownloadCountNormalized = "flatDownloadCountNormalized"
  val flatSearchCountNormalized = "flatSearchCountNormalized"
  val flatCitationCountNormalized = "flatCitationCountNormalized"
  val flatReanalysisCountNormalized = "flatReanalysisCountNormalized"
  val flatViewCountNormalized = "flatViewCountNormalized"
  val flatDownloadCount = "flatDownloadCount"
  val flatSearchCount = "flatSearchCount"
  val flatCitationCount = "flatCitationCount"
  val flatReanalysisCount = "flatReanalysisCount"
  val flatViewCount = "flatViewCount"
  val flatOmicsType = "flatOmicsType"
  val flatTissue    = "flatTissue"
  val flatDisease   = "flatDisease"
  val flatSpecies   = "flatSpecies"
  val finalDomain   = "finalDomain"
  val flatDomain    = "flatDomain"


  val domainCount   = "domainCount"

  val groupOperator = "$group"
  val maxOperator = "$max"
  val minOperator = "$min"
  val aggregate   = "aggregate"
  val pipeline    = "pipeline"
  val matchOperator = "$match"

  val searchScoresCol = "$scores.searchCount"
  val citationScoresCol = "$scores.citationCount"
  val reanalaysisScoresCol = "$scores.reanalysisCount"
  val viewScoresCol = "$scores.viewCount"
  val downloadScoresCol = "$scores.downloadCount"

  val id = "_id"

  val prodMongoUri = config.getString("mongo.produri")
  val devMongoUri = config.getString("mongo.devuri")
  val mongoDatabase =  config.getString("mongo.db")
  val mongoCollection = config.getString("mongo.coll")

  val sparkMasterUrl = config.getString("spark.masterurl")
  val sparkAppName = config.getString("spark.appName")
  val mongOutUriKey = config.getString("spark.mongouturikey")
  val mongoInputUriKey = config.getString("spark.mongoinputurikey")

  val savePath = config.getString("file.path")
  val sparkWriteFormat = config.getString("file.writeformat")
  val connectionFilePath = config.getString("file.connectionfilepath")



}
