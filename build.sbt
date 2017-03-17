name := "Simple Project"

version := "1.0"

scalaVersion := "2.11.8"
val sparkVersion = "2.1.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-twitter" % sparkVersion,
 "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1",
 "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1" classifier "models",
 "org.twitter4j" % "twitter4j-core" % "3.0.3",
 "org.twitter4j" % "twitter4j-stream" % "3.0.3")