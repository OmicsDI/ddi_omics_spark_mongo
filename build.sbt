

name := "omicssparkmongo"

version := "0.1"

scalaVersion := "2.11.9"

val sparkVersion = "2.3.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
    Resolver.url("bintray-sbt-plugins", url("http://dl.bintray.com/sbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
)


libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.apache.spark" %% "spark-mllib" % sparkVersion ,
  "org.apache.spark" %% "spark-streaming" % sparkVersion ,
  "org.apache.spark" %% "spark-hive" % sparkVersion ,
  "org.mongodb.spark" %% "mongo-spark-connector" % sparkVersion ,
  "com.databricks" %% "spark-csv" % "1.5.0" ,
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.4.2" ,
  "org.mongodb" %% "casbah" % "3.1.1" ,
  "com.typesafe" % "config" % "1.3.3" ,
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0" ,
  ("org.apache.spark" %% "spark-core" % sparkVersion )
  //"com.stratio.datasource" %% "spark-mongodb_2.11" % sparkVersion
)

/*libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.mongodb.spark" %% "mongo-spark-connector" % sparkVersion % "provided",
  "com.databricks" %% "spark-csv" % "1.5.0" % "provided",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.4.2" % "provided",
  "org.mongodb" %% "casbah" % "3.1.1" % "provided",
  "com.typesafe" % "config" % "1.3.3" % "provided",
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0" % "provided",
  ("org.apache.spark" %% "spark-core" % sparkVersion )
  //"com.stratio.datasource" %% "spark-mongodb_2.11" % sparkVersion
)*/

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


