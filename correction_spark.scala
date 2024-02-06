// question 1 [1pt]
val dataPath = "/Users/belkacem/Desktop/data"

// reading with full options:
val moviesDf = {
	spark
	.read
	.option("inferSchema", true)
	.option("header", true)
	.option("escape", "\"")
	.option("multiline", true)
	.csv(s"$dataPath/tmdb_5000_movies.csv")
	.cache()
}

val creditsDf = {
	spark
	.read
	.option("inferSchema", true)
	.option("header", true)
	.option("escape", "\"")
	.csv(s"$dataPath/tmdb_5000_credits.csv")
	.cache()
}

// basic reading:
val creditsDf2 = spark.read.option("header", true).csv(s"$dataPath/tmdb_5000_credits.csv")
val moviesDf2 = spark.read.option("header", true).csv(s"$dataPath/tmdb_5000_movies.csv")


// imports
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window




// question 2 [1pt]
{
	moviesDf
	.select("budget", "original_title")
	.orderBy(col("budget").cast(IntegerType).desc)
	.show(false)
}



// question 3 [1pt]
{
	moviesDf
	.select("budget", "original_title", "release_date")
	.where($"release_date" <= "2012-12-31") //.where(to_date(col("release_date"), "yyyy-MM-dd") < to_date(lit("2012-12-31")))
	.orderBy(col("budget").cast(IntegerType).desc)
	.show(false)
}
/*
+---------+-------------------------------------------+------------+
|budget   |original_title                             |release_date|
+---------+-------------------------------------------+------------+
|380000000|Pirates of the Caribbean: On Stranger Tides|2011-05-14  |
|300000000|Pirates of the Caribbean: At World's End   |2007-05-19  |
|270000000|Superman Returns                           |2006-06-28  |
|260000000|John Carter                                |2012-03-07  |
|260000000|Tangled                                    |2010-11-24  |
|258000000|Spider-Man 3                               |2007-05-01  |
|250000000|The Dark Knight Rises                      |2012-07-16  |
|250000000|Harry Potter and the Half-Blood Prince     |2009-07-07  |
|250000000|The Hobbit: An Unexpected Journey          |2012-11-26  |
|237000000|Avatar                                     |2009-12-10  |
|225000000|The Chronicles of Narnia: Prince Caspian   |2008-05-15  |
|225000000|Men in Black 3                             |2012-05-23  |
|220000000|The Avengers                               |2012-04-25  |
|215000000|The Amazing Spider-Man                     |2012-06-27  |
|210000000|X-Men: The Last Stand                      |2006-05-24  |
|209000000|Battleship                                 |2012-04-11  |
|207000000|King Kong                                  |2005-12-14  |
|200000000|Quantum of Solace                          |2008-10-30  |
|200000000|Pirates of the Caribbean: Dead Man's Chest |2006-06-20  |
|200000000|Robin Hood                                 |2010-05-12  |
+---------+-------------------------------------------+------------+
*/


// question 4 [1pt]
{
	moviesDf
	.select("original_title", "original_language", "revenue")
	.where("revenue > 100000000")
	.groupBy("original_language")
	.agg(
		count("original_title").as("nb_movies"),
		sum("revenue").as("tot_revenue"),
		avg("revenue").as("avg_revenue")
	)
	.orderBy(desc("tot_revenue"))
	.limit(5)
	.show(false)
}




// question 5 [3pt]
val countriesSchema = ArrayType(StructType(Array(
	StructField("iso_3166_1", StringType, nullable=true),
	StructField("name", StringType, nullable=true)
)))

{
	moviesDf
	.select("original_title", "production_countries", "budget")
	.withColumn("countries", from_json($"production_countries", countriesSchema))
	.select($"original_title", $"countries.iso_3166_1".as("countries"), $"budget", size($"countries.iso_3166_1").as("nb_countries"))
	.withColumn("budget_per_country", expr("budget/nb_countries"))
	.select($"original_title", explode($"countries").as("country"), $"budget_per_country")
	.groupBy("country")
	.agg(
		sum("budget_per_country").as("total_budget")
	)
	.orderBy(desc("total_budget"))
	.show(30, false)
}



// question 6 [1pt]
{
	moviesDf
	.select("original_title", "revenue", "budget", "release_date")
	.withColumn("year", year($"release_date"))
	.groupBy("year")
	.agg(
		count("original_title").as("nb_movies"),
		sum("revenue").as("tot_revenue"),
		sum("budget").as("tot_budget")
	)
	.withColumn("roi", expr("tot_revenue/tot_budget"))
	.orderBy(desc("year"))
	.show(30, false)
}

/*
+----+---------+-----------+----------+------------------+
|year|nb_movies|tot_revenue|tot_budget|roi               |
+----+---------+-----------+----------+------------------+
|2017|1        |0          |0         |NULL              |
|2016|104      |14461156948|4753140000|3.0424428794439042|
|2015|216      |22775024221|6724547367|3.3868486573187075|
|2014|238      |24120490589|7368453311|3.2734808203224564|
|2013|231      |23411493295|8205880834|2.8530140479249373|
|2012|208      |24141710246|7263782654|3.323572771372187 |
|2011|223      |20516921160|7754227435|2.6459013914646676|
|2010|225      |20348574768|7761467461|2.6217432296467114|
|2009|247      |21072651506|7644466762|2.7565888062657784|
|2008|227      |18146479803|7154949099|2.536213682573397 |
|2007|195      |16491621655|5992647523|2.7519759157708767|
|2006|237      |16635892750|6638529977|2.505960326704419 |
|2005|217      |14931589218|6348584524|2.3519556464205627|
|2004|204      |16278686567|6424797751|2.533727472505461 |
|2003|169      |14213319458|5333938116|2.66469523059611  |
|2002|203      |14609857556|5863946169|2.4914719772216927|
|2001|183      |13269869789|5554016425|2.3892384850122226|
|2000|166      |10952218337|5453200030|2.008402089919302 |
|1999|171      |10332740909|5006425673|2.063895797899325 |
|1998|133      |8223101034 |3900440251|2.1082494551459288|
|1997|112      |9634746300 |3857666953|2.49755782896378  |
|1996|97       |6808007511 |2837092051|2.3996427992529736|
|1995|70       |6207143123 |2116350008|2.932947338359166 |
|1994|55       |5928752504 |1410227000|4.204112177684869 |
|1993|47       |3881952707 |875900000 |4.431958793241238 |
|1992|34       |3756905404 |838625545 |4.479836592623708 |
|1991|30       |2664107492 |723000000 |3.684795977869986 |
|1990|29       |3863771854 |739225000 |5.226787316446278 |
|1989|32       |2686871601 |560260000 |4.795758399671581 |
|1988|31       |1933137768 |323400000 |5.977544118738405 |
+----+---------+-----------+----------+------------------+
*/



// question 7 [1pt]
val fullDf = {
	moviesDf
	.join(creditsDf, moviesDf("id") === creditsDf("movie_id"))
	.cache()
}


// question 8 [2pt]
{
	moviesDf
	.select("original_title", "revenue", "budget", "release_date")
	.withColumn("month_of_year", month($"release_date"))
	.withColumn("year", year($"release_date"))
	.groupBy("month_of_year", "year")
	.agg(
		count("original_title").as("nb_movies")
	)
	.groupBy("month_of_year")
	.agg(
		avg("nb_movies").as("avg_nb_movies")
	)
	.orderBy("month_of_year")
	.show(30, false)
}
/*
+-------------+------------------+
|month_of_year|avg_nb_movies     |
+-------------+------------------+
|NULL         |1.0               |
|1            |9.55              |
|2            |6.653061224489796 |
|3            |7.591836734693878 |
|4            |7.291666666666667 |
|5            |7.408163265306122 |
|6            |6.706896551724138 |
|7            |7.891304347826087 |
|8            |8.428571428571429 |
|9            |13.066666666666666|
|10           |8.172413793103448 |
|11           |6.269230769230769 |
|12           |7.475409836065574 |
+-------------+------------------+
*/


// question 9: 2 ways of understanding the question [1pt or 2pt]
{
	moviesDf
	.select($"original_title", $"popularity", year($"release_date").as("year"))
	.where("year >= 2013")
	.orderBy(desc("popularity"))
	.show(30, false)
}


{
	moviesDf
	.select($"original_title", $"popularity", year($"release_date").as("year"))
	.where("year >= 2013")
	.select(
		$"original_title", $"popularity", $"year",
		row_number.over(Window.partitionBy("year").orderBy(desc("popularity"))).as("popularity_rank")
	)
	.where("popularity_rank <= 5")
	.orderBy(desc("year"), desc("popularity"))
	.show(30, false)
}




// ---
// questions 10-15 bonus 3 points available


// question 10
val productionCompaniesSchema = ArrayType(StructType(Array(
	StructField("id", IntegerType, nullable=true),
	StructField("name", StringType, nullable=true)
)))

{
	moviesDf
	.select("production_companies")
	.withColumn("production_companies2", from_json($"production_companies", productionCompaniesSchema))
	.select(explode($"production_companies2.name").as("production_company"))
	.distinct()
	.orderBy("production_company")
	.show(100, false)
}




// question 11
{
	fullDf
	.select("original_title", "popularity", "vote_average")
	.where($"cast".contains("Christian Bale"))
	.agg(
		avg("popularity") as "avg_popularity",
		avg("vote_average") as "avg_vote"
	)
	.show(false)
}




// question 12 - average number of movies per year per actor
val actorSchema = ArrayType(
	StructType(Array(
		StructField("name", StringType, nullable=true)
	))
)

{
	fullDf
	.select("original_title", "cast")
	.withColumn("cast_json", from_json($"cast", actorSchema))
	.select($"original_title", explode($"cast_json.name").as("actor"))
	.groupBy("actor")
	.agg(
		count("original_title") as "nb_movies"
	)
	.orderBy(desc("nb_movies"))
	.show(20, false)
}
/*
+-----------------+---------+
|actor            |nb_movies|
+-----------------+---------+
|Samuel L. Jackson|67       |
|Robert De Niro   |57       |
|Bruce Willis     |51       |
|Matt Damon       |48       |
|Morgan Freeman   |46       |
|Steve Buscemi    |43       |
|Liam Neeson      |41       |
|Owen Wilson      |40       |
|Johnny Depp      |40       |
|Alec Baldwin     |39       |
|John Goodman     |39       |
|Nicolas Cage     |39       |
|Stanley Tucci    |38       |
|Brad Pitt        |38       |
|Willem Dafoe     |38       |
|Jim Broadbent    |38       |
|Paul Giamatti    |37       |
|Will Ferrell     |37       |
|Richard Jenkins  |36       |
|Susan Sarandon   |36       |
+-----------------+---------+
only showing top 20 rows
*/



// question 13
val genresSchema = ArrayType(StructType(Array(
	StructField("name", StringType, nullable=true)
)))

{
	moviesDf
	.select($"original_title", $"genres", month($"release_date").as("month_of_year"))
	.withColumn("genres_json", from_json($"genres", genresSchema))
	.withColumn("genre", explode($"genres_json.name"))
	.groupBy("month_of_year", "genre")
	.agg(
		count("original_title") as "nb_movies"
	)
	.withColumn("genre_rank", row_number.over(Window.partitionBy("month_of_year").orderBy(desc("nb_movies"))))
	.where("genre_rank <= 3")
	.orderBy("month_of_year", "genre_rank")
	.show(100, false)
}



// question 14
val crewSchema = ArrayType(StructType(Array(
	StructField("name", StringType, nullable=true),
	StructField("job", StringType, nullable=true),
)))

{
	fullDf
	.select($"original_title", $"crew", $"revenue", $"popularity")
	.withColumn("crew_json", explode(from_json($"crew", crewSchema)))
	.drop("crew")
	.where("crew_json.job = 'Director'")
	.select($"original_title", $"crew_json.name" as "director", $"revenue", $"popularity")
	.groupBy("director")
	.agg(
		avg("popularity") as "avg_popularity",
		avg("revenue") as "avg_revenue"
	)
	.withColumn("revenue_rank", rank.over(Window.partitionBy(lit(0)).orderBy(desc("avg_revenue"))))
	.withColumn("popularity_rank", rank.over(Window.partitionBy(lit(0)).orderBy(desc("avg_popularity"))))
	.where("revenue_rank <= 5 or popularity_rank <= 5")
	.show(10, false)
}



// question 15
val keywordsSchema = ArrayType(StructType(Array(
	StructField("name", StringType, nullable=true)
)))

{
	moviesDf
	.select($"original_title", $"keywords")
	.withColumn("keywords_json", from_json($"keywords", keywordsSchema))
	.withColumn("keyword", explode($"keywords_json.name"))
	.dropDuplicates("keyword", "original_title")
	.groupBy("keyword")
	.agg(
		count("*") as "nb_occurences"
	)
	.orderBy(desc("nb_occurences"))
	.show(10, false)
}



