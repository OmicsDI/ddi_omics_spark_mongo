package org.omics.utils

import org.omics.mongoop.SparkMongo.sqlContext
import org.omics.mongoop.{MongoUpdates, OmicsUpdateVocab, SparkMongo}

import scala.collection.mutable

object MainClass {

  def main(args: Array[String]): Unit = {
    val map = mutable.HashMap.empty[String,Double]
    val vocabMap = mutable.HashMap.empty[String,String]
    OmicsUpdateVocab.readOmicsVocab().collect.foreach(r => vocabMap += (r.get(0).toString -> r.get(2).toString))
    val omicsCount = SparkMongo.omicsConDenominators.collect.foreach(r => map += (r.get(0).toString -> r.get(1).toString.toInt))
    SparkMongo.normalizeMetrics(SparkMongo.getProcesseData(SparkMongo.getAggregateData), map, vocabMap)
  }

}
