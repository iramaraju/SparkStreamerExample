name := "weld"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.1"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.1"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.6.1"

libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.2.0"

libraryDependencies += "org.scalanlp" % "breeze_2.10" % "0.11.2"

libraryDependencies += "org.scalanlp" % "breeze-natives_2.10" % "0.11.2"

libraryDependencies += "org.scalanlp" % "breeze-viz_2.10" % "0.11.2"

libraryDependencies += "org.apache.commons" % "commons-csv" % "1.2"

libraryDependencies +=  "com.typesafe.akka" %% "akka-actor" % "2.3.15"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.4.4"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.4.4"

resolvers ++= Seq(
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)
