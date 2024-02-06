// scalastyle:off println

package main.scala.mnm

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.time.LocalDate
import org.apache.spark.sql.expressions.Window

/**
  * Usage: MnMcount <mnm_file_dataset>
  */
object MnMcount {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("MnMCount")
      .getOrCreate()


    val currentDate = LocalDate.now().toString
  
    // path to access file on hdfs
    //val path = s"hdfs://localhost:9000/user/project/datalake/raw/2024-01-22/tmdb_popular_movies.json"
    val path = s"hdfs://localhost:9000/user/project/datalake/raw/$currentDate/tmdb_popular_movies.json"

    //reading the data in a df using option multiline
    val df = spark.read.option("multiline", true)
      .option("header", true).option("inferSchema", true)
      .json(path)

    
    //adding a year column so that the release dates should display as year
    val dfYear = df
      .withColumn("year", year(to_date(col("release_date"), "yyyy-MM-dd")))

    val dfWithYear = dfYear.filter("year is not null")

    //grouping by years so that we can count the number of movies released every year so as to see the progression
    val movieProgression = dfWithYear
      .groupBy("year")
      .agg(count("id").alias("total_movies"))
      .orderBy(col("year").asc)

    //writing the progression of movies with year in our bucket in the parquet format
    movieProgression.write.option("inferSchema", true).mode("overwrite")
      .parquet(s"hdfs://localhost:9000/user/project/datalake/usage/$currentDate/movies_progression_years.parquet")

    movieProgression.show(100)

    //DEUXIEME AXE D'ANALYSE
    //dans selectedDf, nous avons choisi les colonnes qui vont nous servir pour analyser les langues des films les plus regardés
    val selectedDf = dfWithYear.select("year", "original_language", "vote_count", "popularity")

    //Regrouper par année et pas langue les films et recuperer celui avec le max vote_count
    val mostWatchedLanguageDf = selectedDf.groupBy("year", "original_language")
      .agg(max("vote_count").alias("max_vote_count"))
      .orderBy(col("year"), desc("max_vote_count"))

    val windowSpec = Window.partitionBy("year").orderBy(desc("vote_count"))
    val rankedDf = selectedDf.withColumn("rank", row_number().over(windowSpec))

    //supprimer les doublons
    val mostWatchedLanguageFinal = rankedDf.filter("rank = 1").drop("rank")

    mostWatchedLanguageFinal.show(100)

    mostWatchedLanguageFinal.write.option("inferSchema", true).mode("overwrite")
      .parquet(s"hdfs://localhost:9000/user/project/datalake/usage/$currentDate/movies_language/movies_language.parquet")
  }
}
// scalastyle:on println

// val path1 = "gs://demo-bucket-pro/raw/tmdb/popular_movies/2024-01-13/genre_name.json"
// val dfgenre = spark.read.option("multiline", true).option("inferSchema", true).json(path1)
// dfgenre.printSchema(3)
// val explodedDF = dfgenre.select(explode(col("genres")).as("exploded_genre"))
// explodedDF.printSchema(5)
// val finalDf = explodedDF.select(col("exploded_genre.id").as("genre_id"), col("exploded_genre.name").as("genre_name"))
// finalDf.show(4)


// dabord entrer dans le repertoire ensuite zip -r nom_fichier.zip nom_dossier
// df.createOrReplaceTempView("nom_de_table")
// val dfSQL  = spark.sql("")
// val dfPartitionned = df.repartition(5)
// val dfOptimised = df.cache() 
// hdfs dfs -mkdir chemin
// hdfs dfs -chmod chemin
// hdfs dfs -chown user:user chemin

// https://spark.apache.org/docs/latest/sql-data-sources-csv.html 
// https://sparkbyexamples.com/spark/spark-read-options/n 
// https://spark.apache.org/docs/latest/api/scala/org/apache/spark/ContextAwareIterator.html?search=dataframe 
// https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html 

//Top 
//https://spark.apache.org/docs/latest/sql-getting-started.html 
//exemples de toutes les fonctions du dataframes , et jointures 
//https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html 
//Second
//https://spark.apache.org/docs/latest/ 


//Spark programming 
//https://spark.apache.org/docs/latest/sql-programming-guide.html