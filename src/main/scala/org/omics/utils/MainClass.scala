package org.omics.utils

import org.omics.mongoop.SparkMongo.sqlContext
import org.omics.mongoop.{MongoUpdates, SparkMongo}

object MainClass {

  def main(args: Array[String]): Unit = {
    val map = scala.collection.mutable.HashMap.empty[String,Double]
    //import sqlContext.implicits._
    //val broadcastmap = SparkMongo.sqlContext.sparkContext.broadcast(maxminMap)
    val omicsCount = SparkMongo.omicsConDenominators.collect.foreach(r => map += (r.get(0).toString -> r.get(1).toString.toInt))
    SparkMongo.normalizeMetrics(SparkMongo.getProcesseData(SparkMongo.getAggregateData), map)
  }

}
