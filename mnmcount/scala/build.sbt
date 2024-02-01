//name of the package
name := "main/scala/mnm"

//version of our package
version := "1.0"

//version of Scala
scalaVersion := "2.12.18"

// spark library dependencies 
// change this to 3.0.0 when released
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql"  % "3.5.0"
)

libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.18"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.5.0" % "provided"