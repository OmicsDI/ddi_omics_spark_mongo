package org.omics.utils

import org.omics.mongoop.SparkMongo

object MainClass {

  def main(args: Array[String]): Unit = {
    SparkMongo.normalizeMetrics(SparkMongo.getProcesseData(SparkMongo.getAggregateData))
  }

}
