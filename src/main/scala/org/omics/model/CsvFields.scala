package org.omics.model

case class CsvFields(accession:String, database:String, omicsType:Array[String],
                     species:Array[String], tissue:Array[String], disease:Array[String], citationCount:Int,
                     reanalysisCount:Int, searchDomain:Array[String], domainCount:Int,
                     ensembl:Array[String],uniprot:Array[String],viewCount:Int,searchCount:Int,downloadCount:Int
                    )

class MaxMinValues {
  var maxCitationCount = 0
  var minCitationCount = 0
  var maxSearchCount = 0
  var minSearchCount = 0
  var maxReanalysisCount = 0
  var minReanalysisCount = 0
  var maxViewCount = 0
  var minViewCount = 0
  var maxDownloadCount = 0
  var minDownloadCount = 0
}

case class Dataset(accession:String, database: String, connections:String,
                   reanalysis:String = "", view:String = "", citation:String = "",download:String = "")
