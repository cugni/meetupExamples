val logging=Seq(
  "org.apache.logging.log4j" % "log4j-api" % "2.2",
  "org.apache.logging.log4j" % "log4j-core" % "2.2",
  "org.apache.logging.log4j" % "log4j-1.2-api" % "2.2", //bridge between log4j v1 to v2
  "org.apache.logging.log4j" % "log4j-jcl" % "2.2",
  "org.slf4j" % "slf4j-api" % "1.7.10",
  "org.slf4j" % "slf4j-log4j12" % "1.7.10"
  )
val testing=Seq(
  "junit" % "junit" % "4.12" % "test",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)
lazy val meetupExamples = (project in file(".")).
  settings(
    javacOptions ++= Seq("-source", "1.7", "-target", "1.7"),
    name := "meetupExamples",
    version := "1.0",
    scalaVersion := "2.10.4",
    scalacOptions ++= Seq("-deprecation", "-feature"),
    libraryDependencies += "org.specs2" %% "specs2" % "2.2"  ,
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0"  ,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.1.0"  ,
    // the assembly of the connector better to add separately.
    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.1.1" ,
    libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.5",
    // Dependencies for logging
   // libraryDependencies ++= logging,
    libraryDependencies ++= testing,
    //Let's see if we can avoid hadoop at all
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.2.0"  , //Dont' forget to add this dependency, otherwise strange EOF exception may come up
    libraryDependencies += "com.typesafe" % "config" % "1.2.1"
  )
