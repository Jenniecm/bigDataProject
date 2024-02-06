

// -----------------------
// ------ TP1 SPARK ------
// -----------------------








// -----------------------
// -- READ / WRITE DATA --
// -----------------------
// In this section we oversee different options to read files.
// We also discuss how to define schemas and different options to build a typed DataFrame.


// read one CSV file alone
// by default Spark reads all files in a given repository
val archivePath = "/Users/belkacem/Downloads/archive"
val players22 = spark.read.option("header", true).csv(s"$archivePath/players_22.csv")


// verify content and show schema
players22.printSchema
players22.show()
players22.count()


// focus on name, value, wage and nationality
// let's write this data in a new folder
// notice we also have to define the header=true option with CSV
players22.select("short_name", "value_eur", "wage_eur", "nationality_name").show()
/*
+-----------------+---------+--------+----------------+
|       short_name|value_eur|wage_eur|nationality_name|
+-----------------+---------+--------+----------------+
|         L. Messi|    7.8E7|320000.0|       Argentina|
|   R. Lewandowski|  1.195E8|270000.0|          Poland|
|Cristiano Ronaldo|    4.5E7|270000.0|        Portugal|
|        Neymar Jr|   1.29E8|270000.0|          Brazil|
|     K. De Bruyne|  1.255E8|350000.0|         Belgium|
|         J. Oblak|   1.12E8|130000.0|        Slovenia|
|        K. Mbappé|   1.94E8|230000.0|          France|
|         M. Neuer|   1.35E7| 86000.0|         Germany|
|    M. ter Stegen|    9.9E7|250000.0|         Germany|
|          H. Kane|  1.295E8|240000.0|         England|
|         N. Kanté|    1.0E8|230000.0|          France|
|       K. Benzema|    6.6E7|350000.0|          France|
|      T. Courtois|   8.55E7|250000.0|         Belgium|
|           H. Son|   1.04E8|220000.0|  Korea Republic|
|         Casemiro|    8.8E7|310000.0|          Brazil|
|      V. van Dijk|    8.6E7|230000.0|     Netherlands|
|          S. Mané|   1.01E8|270000.0|         Senegal|
|         M. Salah|   1.01E8|270000.0|           Egypt|
|          Ederson|    9.4E7|200000.0|          Brazil|
|       J. Kimmich|   1.08E8|160000.0|         Germany|
+-----------------+---------+--------+----------------+
*/

{
     players22
     .select("short_name", "value_eur", "wage_eur", "nationality_name")
     .write
     .option("header", true)
     .csv(s"$archivePath/players22_simplified_csv/")
}


// notice that saving creates a folder not a file - it's normal
// other examples of saving in json and parquet formats
players22.select("short_name", "value_eur", "wage_eur", "nationality_name").write.json(s"$archivePath/players22_simplified_json/")
players22.select("short_name", "value_eur", "wage_eur", "nationality_name").write.parquet(s"$archivePath/players22_simplified_parquet/")


// let's define a proper schema
// we need to import types first
import org.apache.spark.sql.types._

val playersSchemaSimplified = StructType(Array(
     StructField("short_name", StringType, true),
     StructField("value_eur", IntegerType, true),
     StructField("wage_eur", IntegerType, true),
     StructField("nationality_name", StringType, true)
))


// let's read this new data while specifying the schema
val players22Simplified = {
     spark.read
     .schema(playersSchemaSimplified)
     .option("header", true)
     .csv(s"$archivePath/players22_simplified_csv/")
}

players22Simplified.show()
players22Simplified.printSchema

/*
+---------------+---------+--------+----------------+                           
|     short_name|value_eur|wage_eur|nationality_name|
+---------------+---------+--------+----------------+
|      R. Guerra|     NULL|    NULL|         Uruguay|
| D. Butterworth|     NULL|    NULL|         England|
|   B. Janošević|     NULL|    NULL|          Serbia|
|        J. Amon|     NULL|    NULL|   United States|
|       J. Opoku|     NULL|    NULL|         England|
|      A. Karimi|     NULL|    NULL|            Iran|
|      L. Offord|     NULL|    NULL|         England|
|    Lucas Cunha|     NULL|    NULL|          Brazil|
|     A. Heredia|     NULL|    NULL|       Argentina|
|        R. Leak|     NULL|    NULL|           Wales|
|       F. Kusić|     NULL|    NULL|          Serbia|
|      L. Pierre|     NULL|    NULL|           Haiti|
|        E. Watt|     NULL|    NULL|        Scotland|
|     R. Germain|     NULL|    NULL|           Japan|
|   J. Marulanda|     NULL|    NULL|        Colombia|
|M. Al Shanqeeti|     NULL|    NULL|    Saudi Arabia|
|        M. Real|     NULL|    NULL|   United States|
|     L. Hannant|     NULL|    NULL|         England|
|   A. Al Hamdan|     NULL|    NULL|    Saudi Arabia|
|    L. D'Arrigo|     NULL|    NULL|       Australia|
+---------------+---------+--------+----------------+
*/


// it doesn't work properly: this is because the columns have been specified as IntegerType instead of DoubleType.
// Also the column order is important as well as types definition for CSV.
// to avoid that we have the possibility to inferSchema - however it's slower as Spark must do 2 passes over the data
// remark : it's very important to have proper schema to be able to compute statistics.
// eg. impossible to compute an average on strings
// let's re-build this dataset properly

val players22Simplified = spark.read.option("inferSchema", true).option("header", true).csv(s"$archivePath/players22_simplified_csv/")
players22Simplified.show()
players22Simplified.printSchema

/*
+---------------+---------+--------+----------------+
|     short_name|value_eur|wage_eur|nationality_name|
+---------------+---------+--------+----------------+
|      R. Guerra| 850000.0|  5000.0|         Uruguay|
| D. Butterworth|1300000.0|  8000.0|         England|
|   B. Janošević| 300000.0|  2000.0|          Serbia|
|        J. Amon|1200000.0|  3000.0|   United States|
|       J. Opoku|1200000.0|  9000.0|         England|
|      A. Karimi| 675000.0|  4000.0|            Iran|
|      L. Offord|1100000.0|  2000.0|         England|
|    Lucas Cunha|1100000.0|  2000.0|          Brazil|
|     A. Heredia|1100000.0|   500.0|       Argentina|
|        R. Leak|1100000.0|  2000.0|           Wales|
|       F. Kusić| 825000.0|  1000.0|          Serbia|
|      L. Pierre|1100000.0|  1000.0|           Haiti|
|        E. Watt|1200000.0|  3000.0|        Scotland|
|     R. Germain| 725000.0|  1000.0|           Japan|
|   J. Marulanda| 725000.0|   600.0|        Colombia|
|M. Al Shanqeeti|1300000.0|  7000.0|    Saudi Arabia|
|        M. Real|1300000.0|  1000.0|   United States|
|     L. Hannant| 625000.0|  4000.0|         England|
|   A. Al Hamdan|1300000.0|  7000.0|    Saudi Arabia|
|    L. D'Arrigo|1300000.0|   650.0|       Australia|
+---------------+---------+--------+----------------+

root
 |-- short_name: string (nullable = true)
 |-- value_eur: double (nullable = true)
 |-- wage_eur: double (nullable = true)
 |-- nationality_name: string (nullable = true)
*/

// we now understand the problem we had earlier - it's because of column types it was expecting Double but got Integer. Let's fix that
val playersSchemaSimplified = StructType(Array(
     StructField("short_name", StringType, true),
     StructField("value_eur", DoubleType, true),
     StructField("wage_eur", DoubleType, true),
     StructField("nationality_name", StringType, true)))

val players22Simplified = spark.read.schema(playersSchemaSimplified).option("header", true).csv(s"$archivePath/players22_simplified_csv/")
players22Simplified.printSchema
players22Simplified.show()

// the data is now properly read even if we specify the schema
// one option could be to read a first time the data, explore it and then build a schema properly to optimise next readings of the data.


val players22SimplifiedJson = spark.read.schema(playersSchemaSimplified).option("header", true).json(s"$archivePath/players22_simplified_json/")
players22SimplifiedJson.show()
// let's try reading the Json data using this schema
// is it working properly? why?
// --> reading Json files with this schema is not working because when saving we saved as string.

// let's re-save again but using the DF with the properly defined schema
players22Simplified.printSchema

{
     players22Simplified
     .select("short_name", "value_eur", "wage_eur", "nationality_name")
     .write
     .mode("overwrite")
     .json(s"$archivePath/players22_simplified_json/")
}

// note use of mode "overwrite" to enforce writing even if this path already exists

val players22SimplifiedJson = {
     spark.read
     .schema(playersSchemaSimplified)
     .option("header", true)
     .json(s"$archivePath/players22_simplified_json/")
}
players22SimplifiedJson.show()
// now the columns are properly interpreted in Json

val players22SimplifiedParquet = spark.read.schema(playersSchemaSimplified).parquet(s"$archivePath/players22_simplified_parquet/")
players22SimplifiedParquet.show()


// Not working. "Column: [value_eur], Expected: double, Found: BINARY."
// Same issue as we had with the Json one.
// With Parqut it's not needed to provide the schema as it's part of the file content.


val players22SimplifiedParquet = spark.read.parquet(s"$archivePath/players22_simplified_parquet/")
players22SimplifiedParquet.show()
players22SimplifiedParquet.printSchema


// let's check the content of one column = use describe to get sense of what's inside
// describe returns a dataframe so we have to show it
players22SimplifiedParquet.describe("value_eur").show()



// we are now ready to do the analysis and exploration of the data.





// --------------------------
// -- ANALYSIS OF THE DATA --
// -------- LEVEL 1 ---------
// --------------------------


/*
val archivePath = "/Users/belkacem/Downloads/archive"

val players22 = {
     spark.read
     .option("header", true)
     .option("inferSchema", true)
     .csv(s"$archivePath/players_22.csv")
}
*/




// 1. preferred foot
players22.select("preferred_foot").groupBy("preferred_foot").count().show()
players22.select("preferred_foot").groupBy("preferred_foot").agg(sum(lit(1))).show()



// 2. salaries 2015 vs 2022
val players15 = spark.read.option("header", true).option("inferSchema", true).csv(s"$archivePath/players_15.csv")
players15.select("wage_eur").agg(avg("wage_eur") as "avg_wage_eur").show()
players22.select("wage_eur").agg(avg("wage_eur") as "avg_wage_eur").show()




// 3. top 20 best players based on global grade with salaries and values
players22.select("short_name", "overall", "potential", "value_eur", "wage_eur").sort(desc("overall")).show()

// warning : players22 has string columns so the sorting is alphabetical not numerical ! (it is important to have proper schema)
// let's use expressions to convert those columns
players22.selectExpr(
     "short_name", "cast(overall as int) overall", "cast(potential as int) potential",
     "cast(value_eur as double) value_eur", "cast(wage_eur as double) wage_eur"
).sort(desc("wage_eur")).show()

/*
players by wage:
+-----------------+-------+---------+---------+--------+
|       short_name|overall|potential|value_eur|wage_eur|
+-----------------+-------+---------+---------+--------+
|     K. De Bruyne|     91|       91|  1.255E8|350000.0|
|       K. Benzema|     89|       89|    6.6E7|350000.0|
|         L. Messi|     93|       93|    7.8E7|320000.0|
|         Casemiro|     89|       89|    8.8E7|310000.0|
|         T. Kroos|     88|       88|    7.5E7|310000.0|
|      R. Sterling|     88|       89|  1.075E8|290000.0|
|   R. Lewandowski|     92|       92|  1.195E8|270000.0|
|Cristiano Ronaldo|     91|       91|    4.5E7|270000.0|
|        Neymar Jr|     91|       91|   1.29E8|270000.0|
|          S. Mané|     89|       89|   1.01E8|270000.0|
|         M. Salah|     89|       89|   1.01E8|270000.0|
|        R. Lukaku|     88|       88|   9.35E7|260000.0|
|        S. Agüero|     87|       87|    5.1E7|260000.0|
|    M. ter Stegen|     90|       92|    9.9E7|250000.0|
|      T. Courtois|     89|       91|   8.55E7|250000.0|
|  Bruno Fernandes|     88|       89|  1.075E8|250000.0|
|          H. Kane|     90|       90|  1.295E8|240000.0|
|        E. Hazard|     85|       85|    5.2E7|240000.0|
|        K. Mbappé|     91|       95|   1.94E8|230000.0|
|         N. Kanté|     90|       90|    1.0E8|230000.0|
+-----------------+-------+---------+---------+--------+

players by value
+-------------------+-------+---------+---------+--------+
|         short_name|overall|potential|value_eur|wage_eur|
+-------------------+-------+---------+---------+--------+
|          K. Mbappé|     91|       95|   1.94E8|230000.0|
|         E. Haaland|     88|       93|  1.375E8|110000.0|
|            H. Kane|     90|       90|  1.295E8|240000.0|
|          Neymar Jr|     91|       91|   1.29E8|270000.0|
|       K. De Bruyne|     91|       91|  1.255E8|350000.0|
|      G. Donnarumma|     89|       93|  1.195E8|110000.0|
|     R. Lewandowski|     92|       92|  1.195E8|270000.0|
|         F. de Jong|     87|       92|  1.195E8|210000.0|
|          J. Sancho|     87|       91|  1.165E8|150000.0|
|T. Alexander-Arnold|     87|       92|   1.14E8|150000.0|
|           J. Oblak|     91|       93|   1.12E8|130000.0|
|         J. Kimmich|     89|       90|   1.08E8|160000.0|
|        R. Sterling|     88|       89|  1.075E8|290000.0|
|    Bruno Fernandes|     88|       89|  1.075E8|250000.0|
|             H. Son|     89|       89|   1.04E8|220000.0|
|         Rúben Dias|     87|       91|  1.025E8|170000.0|
|            S. Mané|     89|       89|   1.01E8|270000.0|
|           M. Salah|     89|       89|   1.01E8|270000.0|
|           N. Kanté|     90|       90|    1.0E8|230000.0|
|      M. ter Stegen|     90|       92|    9.9E7|250000.0|
+-------------------+-------+---------+---------+--------+

==> Mbappé or Haaland are paid less than Hazard but have higher market value, grade and potential.
==> Ronaldo is paid as much as Neymar but has a value that is twice/thrice lower.
*/



// 4. players potential 15 vs 22
players15.selectExpr(
     "short_name", "cast(overall as int) overall", "cast(potential as int) potential"
).sort(desc("potential")).show()

players22.selectExpr(
     "short_name", "cast(overall as int) overall", "cast(potential as int) potential"
).sort(desc("overall")).show()

// beware to CONVERT otherwise orderBy might give you inconsistent result

/*
2015 most potential
+-----------------+-------+---------+                                           
|       short_name|overall|potential|
+-----------------+-------+---------+
|         L. Messi|     93|       95|
|Cristiano Ronaldo|     92|       92|
|     J. Rodríguez|     86|       92|
|        L. Suárez|     89|       91|
|          G. Bale|     87|       91|
|           Neymar|     86|       91|
|         M. Götze|     85|       91|
|        A. Robben|     90|       90|
|   Z. Ibrahimović|     90|       90|
|         M. Neuer|     90|       90|
|        E. Hazard|     88|       90|
|      T. Courtois|     86|       90|
|   R. Lewandowski|     87|       89|
|  Sergio Busquets|     85|       89|
|          Iniesta|     89|       89|
|            Piqué|     84|       89|
|         P. Pogba|     83|       89|
|         T. Kroos|     85|       88|
|       A. Sánchez|     84|       88|
|        F. Ribéry|     88|       88|
+-----------------+-------+---------+

2022 best grades
+-----------------+-------+---------+                                           
|       short_name|overall|potential|
+-----------------+-------+---------+
|         L. Messi|     93|       93|
|   R. Lewandowski|     92|       92|
|Cristiano Ronaldo|     91|       91|
|        Neymar Jr|     91|       91|
|     K. De Bruyne|     91|       91|
|         J. Oblak|     91|       93|
|        K. Mbappé|     91|       95|
|         M. Neuer|     90|       90|
|    M. ter Stegen|     90|       92|
|          H. Kane|     90|       90|
|         N. Kanté|     90|       90|
|       K. Benzema|     89|       89|
|      T. Courtois|     89|       91|
|           H. Son|     89|       89|
|         Casemiro|     89|       89|
|      V. van Dijk|     89|       89|
|          S. Mané|     89|       89|
|         M. Salah|     89|       89|
|          Ederson|     89|       91|
|       J. Kimmich|     89|       90|
+-----------------+-------+---------+

==> Hard to read this way - let's join together (5. optional)
*/


val playersPotential = {
     players15.selectExpr(
          "sofifa_id", "short_name", "cast(overall as int) overall_15", "cast(potential as int) potential_15"
     ).join(players22.selectExpr(
          "sofifa_id", "short_name", "cast(overall as int) overall_22", "cast(potential as int) potential_22"
     ), "sofifa_id")
     .withColumn("evolution", expr("overall_22-overall_15"))
     .withColumn("potential_vs_actual", col("overall_22")-col("potential_15"))
}

{
     playersPotential
     .orderBy(desc("evolution"))
     .show()
}

/*
+---------+-----------------+----------+------------+-----------------+----------+------------+---------+-------------------+
|sofifa_id|       short_name|overall_15|potential_15|       short_name|overall_22|potential_22|evolution|potential_vs_actual|
+---------+-----------------+----------+------------+-----------------+----------+------------+---------+-------------------+
|   158023|         L. Messi|        93|          95|         L. Messi|        93|          93|        0|                 -2|
|   188545|   R. Lewandowski|        87|          89|   R. Lewandowski|        92|          92|        5|                  3|
|    20801|Cristiano Ronaldo|        92|          92|Cristiano Ronaldo|        91|          91|       -1|                 -1|
|   190871|           Neymar|        86|          91|        Neymar Jr|        91|          91|        5|                  0|
|   192985|     K. De Bruyne|        81|          86|     K. De Bruyne|        91|          91|       10|                  5|
|   200389|         J. Oblak|        77|          82|         J. Oblak|        91|          93|       14|                  9|
|   167495|         M. Neuer|        90|          90|         M. Neuer|        90|          90|        0|                  0|
|   192448|    M. ter Stegen|        82|          88|    M. ter Stegen|        90|          92|        8|                  2|
|   202126|          H. Kane|        71|          78|          H. Kane|        90|          90|       19|                 12|
|   215914|         N. Kanté|        72|          76|         N. Kanté|        90|          90|       18|                 14|
|   165153|       K. Benzema|        85|          87|       K. Benzema|        89|          89|        4|                  2|
|   192119|      T. Courtois|        86|          90|      T. Courtois|        89|          91|        3|                 -1|
|   200104|           H. Son|        76|          81|           H. Son|        89|          89|       13|                  8|
|   200145|         Casemiro|        76|          80|         Casemiro|        89|          89|       13|                  9|
|   203376|      V. van Dijk|        75|          79|      V. van Dijk|        89|          89|       14|                 10|
|   208722|          S. Mané|        74|          80|          S. Mané|        89|          89|       15|                  9|
|   209331|         M. Salah|        76|          84|         M. Salah|        89|          89|       13|                  5|
|   210257|          Ederson|        69|          77|          Ederson|        89|          91|       20|                 12|
|   212622|       J. Kimmich|        63|          78|       J. Kimmich|        89|          90|       26|                 11|
|   155862|     Sergio Ramos|        87|          87|     Sergio Ramos|        88|          88|        1|                  1|
+---------+-----------------+----------+------------+-----------------+----------+------------+---------+-------------------+
*/


// potentiel_vs_actual with absolute value --> eg. potential_vs_actual close to 1? (< 1 or > -1)




// players that reached or went above their potential
{
     playersPotential
     .where(expr("potential_vs_actual >= 0"))
     .orderBy(desc("potential_vs_actual"))
     .show()
}

/*
+---------+-------------------+----------+------------+-------------------+----------+------------+---------+-------------------+
|sofifa_id|         short_name|overall_15|potential_15|         short_name|overall_22|potential_22|evolution|potential_vs_actual|
+---------+-------------------+----------+------------+-------------------+----------+------------+---------+-------------------+
|   224179|     Borja Iglesias|        50|          55|     Borja Iglesias|        80|          80|       30|                 25|
|   225375|          K. Laimer|        51|          57|          K. Laimer|        81|          85|       30|                 24|
|   225024|         M. Holgate|        47|          53|         M. Holgate|        77|          81|       30|                 24|
|   221479|   D. Calvert-Lewin|        51|          60|   D. Calvert-Lewin|        81|          85|       30|                 21|
|   220407|        M. Dúbravka|        58|          61|        M. Dúbravka|        81|          81|       23|                 20|
|   223848|S. Milinković-Savić|        57|          65|S. Milinković-Savić|        85|          87|       28|                 20|
|   222123|            A. Long|        48|          55|            A. Long|        75|          77|       27|                 20|
|   203841|            N. Pope|        57|          64|            N. Pope|        83|          83|       26|                 19|
|   221697|         O. Watkins|        49|          59|         O. Watkins|        78|          83|       29|                 19|
|   224415|         Luis Rioja|        49|          54|         Luis Rioja|        73|          73|       24|                 19|
|   194372|        M. Taşkıran|        43|          49|        E. Taşkıran|        67|          67|       24|                 18|
|   225193|       Mikel Merino|        59|          65|             Merino|        83|          87|       24|                 18|
|   225252|           J. Duque|        49|          56|           J. Duque|        74|          74|       25|                 18|
|   221488|    R. Hollingshead|        52|          54|    R. Hollingshead|        72|          72|       20|                 18|
|   223905|              P. Ng|        46|          52|              P. Ng|        70|          74|       24|                 18|
|   221087|                Pau|        57|          63|          Pau López|        80|          80|       23|                 17|
|   221491|          N. Elvedi|        50|          62|          N. Elvedi|        79|          83|       29|                 17|
|   223249|           G. Arias|        58|          62|           G. Arias|        79|          79|       21|                 17|
|   224444|        H. Belkebla|        54|          58|        H. Belkebla|        75|          77|       21|                 17|
|   224301|       K. Świderski|        50|          56|       K. Świderski|        73|          78|       23|                 17|
+---------+-------------------+----------+------------+-------------------+----------+------------+---------+-------------------+
*/


// players with the best evolution
playersPotential.orderBy(desc("evolution")).show()
/*
+---------+-------------------+----------+------------+-------------------+----------+------------+---------+-------------------+
|sofifa_id|         short_name|overall_15|potential_15|         short_name|overall_22|potential_22|evolution|potential_vs_actual|
+---------+-------------------+----------+------------+-------------------+----------+------------+---------+-------------------+
|   224179|     Borja Iglesias|        50|          55|     Borja Iglesias|        80|          80|       30|                 25|
|   221479|   D. Calvert-Lewin|        51|          60|   D. Calvert-Lewin|        81|          85|       30|                 21|
|   225375|          K. Laimer|        51|          57|          K. Laimer|        81|          85|       30|                 24|
|   225024|         M. Holgate|        47|          53|         M. Holgate|        77|          81|       30|                 24|
|   221491|          N. Elvedi|        50|          62|          N. Elvedi|        79|          83|       29|                 17|
|   221697|         O. Watkins|        49|          59|         O. Watkins|        78|          83|       29|                 19|
|   211515|         P. Gollini|        54|          67|         P. Gollini|        82|          87|       28|                 15|
|   223848|S. Milinković-Savić|        57|          65|S. Milinković-Savić|        85|          87|       28|                 20|
|   215698|         M. Maignan|        56|          74|         M. Maignan|        84|          89|       28|                 10|
|   225100|           J. Gomez|        55|          74|           J. Gomez|        82|          88|       27|                  8|
|   215502|       B. Drągowski|        50|          68|       B. Drągowski|        77|          81|       27|                  9|
|   222123|            A. Long|        48|          55|            A. Long|        75|          77|       27|                 20|
|   203841|            N. Pope|        57|          64|            N. Pope|        83|          83|       26|                 19|
|   212622|         J. Kimmich|        63|          78|         J. Kimmich|        89|          90|       26|                 11|
|   220697|        J. Maddison|        56|          74|        J. Maddison|        82|          85|       26|                  8|
|   211117|            D. Alli|        55|          76|            D. Alli|        80|          82|       25|                  4|
|   225252|           J. Duque|        49|          56|           J. Duque|        74|          74|       25|                 18|
|   223329|           A. Conti|        51|          63|           A. Conti|        76|          77|       25|                 13|
|   212287|           A. Maxsø|        50|          63|           A. Maxsø|        75|          77|       25|                 12|
|   221370|       M. Al Buraik|        47|          55|       M. Al Buraik|        72|          72|       25|                 17|
+---------+-------------------+----------+------------+-------------------+----------+------------+---------+-------------------+
*/

// ==> lots of players were clearly under-valued





// 5. players by country
val playersNationalities = players22.selectExpr(
     "short_name", "cast(overall as int) score", "cast(potential as int)",
     "cast(wage_eur as int)", "cast(value_eur as int)", "nationality_name"
)

{
     playersNationalities
     .select("nationality_name")
     .groupBy("nationality_name")
     .count()
     .orderBy(desc("count"))
     .show()
}

/*
+-------------------+-----+
|   nationality_name|count|
+-------------------+-----+
|            England| 1719|
|            Germany| 1214|
|              Spain| 1086|
|             France|  980|
|          Argentina|  960|
|             Brazil|  897|
|              Japan|  546|
|        Netherlands|  439|
|      United States|  413|
|             Poland|  403|
|             Sweden|  385|
|           China PR|  385|
|             Norway|  379|
|Republic of Ireland|  374|
|           Portugal|  373|
|       Saudi Arabia|  355|
|             Mexico|  352|
|              Italy|  338|
|            Romania|  338|
|     Korea Republic|  325|
+-------------------+-----+
*/

{
     playersNationalities
     .select("nationality_name")
     .groupBy("nationality_name")
     .count()
     .orderBy(asc("count"))
     .show()
}

// ex: how many countries have less than 10 players
// ex: how many countries have less than 1 players

/*
+----------------+-----+
|nationality_name|count|
+----------------+-----+
|        Ethiopia|    1|
|            Fiji|    1|
|         Andorra|    1|
|        Tanzania|    1|
|         Bermuda|    1|
|         Eritrea|    1|
|          Belize|    1|
|       Korea DPR|    1|
|            Guam|    1|
|     Saint Lucia|    1|
|      Kyrgyzstan|    1|
|Papua New Guinea|    1|
|       Mauritius|    1|
|     Afghanistan|    1|
|        Barbados|    1|
|          Bhutan|    1|
|       Gibraltar|    1|
|         Estonia|    1|
|         Vietnam|    1|
|       Indonesia|    1|
+----------------+-----+
*/


{
     playersNationalities
     .select("nationality_name", "score")
     .groupBy("nationality_name")
     .agg(avg("score").alias("avg_score"))
     .orderBy(desc("avg_score"))
     .show()
}

/*
+--------------------+-----------------+
|    nationality_name|        avg_score|
+--------------------+-----------------+
|            Tanzania|             74.0|
|               Libya|73.33333333333333|
|          Mozambique|             73.0|
|Central African R...|             72.5|
|               Egypt|            72.25|
|                Fiji|             72.0|
|               Syria|             72.0|
|               Gabon|71.16666666666667|
|              Brazil|70.85172798216277|
|      Czech Republic| 70.6826923076923|
|             Algeria| 70.6470588235294|
|             Ukraine| 70.5072463768116|
|            Suriname|70.27272727272727|
|             Bermuda|             70.0|
|               Italy|69.98224852071006|
|            Portugal|69.72654155495978|
|             Namibia|69.66666666666667|
|               Spain|69.56353591160222|
|             Morocco|69.48514851485149|
|                Iran|69.42857142857143|
+--------------------+-----------------+
*/

// let's check on how many people this statistic is computed

{
     playersNationalities
     .select("nationality_name", "score")
     .groupBy("nationality_name")
     .agg(avg("score").alias("avg_score"), count(lit(1)).alias("count"))
     .orderBy(desc("avg_score"))
     .show()
}

/*
+--------------------+-----------------+-----+                                  
|    nationality_name|        avg_score|count|
+--------------------+-----------------+-----+
|            Tanzania|             74.0|    1|
|               Libya|73.33333333333333|    3|
|          Mozambique|             73.0|    4|
|Central African R...|             72.5|    2|
|               Egypt|            72.25|   12|
|                Fiji|             72.0|    1|
|               Syria|             72.0|    2|
|               Gabon|71.16666666666667|   12|
|              Brazil|70.85172798216277|  897|
|      Czech Republic| 70.6826923076923|  104|
|             Algeria| 70.6470588235294|   51|
|             Ukraine| 70.5072463768116|   69|
|            Suriname|70.27272727272727|   11|
|             Bermuda|             70.0|    1|
|               Italy|69.98224852071006|  338|
|            Portugal|69.72654155495978|  373|
|             Namibia|69.66666666666667|    3|
|               Spain|69.56353591160222| 1086|
|             Morocco|69.48514851485149|  101|
|                Iran|69.42857142857143|   21|
+--------------------+-----------------+-----+
*/


// let's consider only significative counts


{
     playersNationalities
     .select("nationality_name", "score")
     .groupBy("nationality_name")
     .agg(
          avg("score").alias("avg_score"),
          count(lit(1)).alias("count")
     )
     .where(expr("count > 50"))
     .orderBy(desc("avg_score"))
     // .agg(sum("count"))
     .show()
}

{
     playersNationalities
     .select("nationality_name", "score", "short_name")
     .groupBy("nationality_name")
     .agg(
          avg("score").as("overall_avg"),
          count("short_name").as("nb_players") // an alternative to count players
     )
     .orderBy(desc("overall_avg"))
     .where("nb_players > 50")
     // .agg(sum("nb_players"))
     .show()
}

/*
+----------------+-----------------+-----+
|nationality_name|        avg_score|count|
+----------------+-----------------+-----+
|           Egypt|            72.25|   12|
|           Gabon|71.16666666666667|   12|
|          Brazil|70.85172798216277|  897|
|  Czech Republic| 70.6826923076923|  104|
|         Algeria| 70.6470588235294|   51|
|         Ukraine| 70.5072463768116|   69|
|        Suriname|70.27272727272727|   11|
|           Italy|69.98224852071006|  338|
|        Portugal|69.72654155495978|  373|
|           Spain|69.56353591160222| 1086|
|         Morocco|69.48514851485149|  101|
|            Iran|69.42857142857143|   21|
|      Montenegro|69.30434782608695|   23|
|          Serbia|69.11382113821138|  123|
|         Croatia|69.08441558441558|  154|
|        Slovakia|68.96666666666667|   60|
|          Russia|68.95698924731182|   93|
|        Slovenia|68.92307692307692|   52|
|          Greece|68.76530612244898|   98|
|            Togo|68.72727272727273|   11|
+----------------+-----------------+-----+
*/





// same for wage and value:


{
     playersNationalities
     .select("nationality_name", "wage_eur")
     .groupBy("nationality_name")
     .agg(
          avg("wage_eur").alias("avg_wage"),
          count(lit(1)).alias("count")
     )
     .where(expr("count > 10"))
     .orderBy(desc("avg_wage"))
     .show()
}

/*
+----------------+------------------+-----+
|nationality_name|          avg_wage|count|
+----------------+------------------+-----+
|           Egypt|43041.666666666664|   12|
|           Gabon|20808.333333333332|   12|
|           Italy|20603.698224852073|  338|
|         Algeria|20552.941176470587|   51|
|          Russia| 17825.30864197531|   93|
|          Brazil|16266.220735785953|  897|
|         Jamaica|14783.333333333334|   36|
|           Spain|14599.217311233886| 1086|
|      Montenegro|14391.304347826086|   23|
|         Senegal|14390.157480314962|  127|
|         Belgium|       14214.21875|  321|
|        Portugal|13966.621983914209|  373|
|         Croatia|13317.532467532468|  154|
|          France|13164.387755102041|  980|
|        Slovenia| 13069.23076923077|   52|
|          Serbia|13001.219512195123|  123|
|         Morocco|12904.455445544554|  101|
|        Cameroon|12713.846153846154|   65|
|   Côte d'Ivoire|12595.495495495496|  111|
|        Zimbabwe|12307.692307692309|   13|
+----------------+------------------+-----+
*/



{
     playersNationalities
     .select("nationality_name", "value_eur")
     .groupBy("nationality_name")
     .agg(
          avg("value_eur").alias("avg_value"),
          count(lit(1)).alias("count")
     )
     .where(expr("count > 10"))
     .orderBy(desc("avg_value"))
     .show()
}

/*
+----------------+------------------+-----+
|nationality_name|         avg_value|count|
+----------------+------------------+-----+
|           Egypt|        1.201875E7|   12|
|           Italy| 6320961.538461538|  338|
|         Algeria| 6217549.019607843|   51|
|        Portugal| 6086689.008042895|  373|
|           Gabon|         5975000.0|   12|
|           Spain| 5303755.760368664| 1086|
|    Burkina Faso|         5296875.0|   16|
|        Slovenia| 5109615.384615385|   52|
|          Brazil| 5102982.162764772|  897|
|         Ukraine| 4872238.805970149|   69|
|         Morocco|4820693.0693069305|  101|
|          France| 4698387.755102041|  980|
|         Croatia| 4614707.792207792|  154|
|          Serbia| 4566626.016260163|  123|
|            Togo|4502272.7272727275|   11|
|  Czech Republic| 4403186.274509804|  104|
|         Senegal| 4370354.330708661|  127|
|     Netherlands| 4335592.255125285|  439|
|         Belgium|       4270578.125|  321|
|      Montenegro| 4231521.739130435|   23|
+----------------+------------------+-----+
*/





// 6. best potential per country

{
     playersNationalities
     .groupBy("nationality_name")
     .agg(
          max_by(col("short_name"), col("potential")).as("country_best"),
          max("potential").as("max_potential")
     )
     .orderBy(desc("max_potential"))
     .show(10)
}
/*
+----------------+--------------+-------------+                                 
|nationality_name|  country_best|max_potential|
+----------------+--------------+-------------+
|          France|     K. Mbappé|           95|
|       Argentina|      L. Messi|           93|
|           Italy| G. Donnarumma|           93|
|          Norway|    E. Haaland|           93|
|        Slovenia|      J. Oblak|           93|
|     Netherlands|    F. de Jong|           92|
|         England|      P. Foden|           92|
|         Germany|    K. Havertz|           92|
|          Poland|R. Lewandowski|           92|
|         Belgium|   T. Courtois|           91|
+----------------+--------------+-------------+
*/





// 7. evolution of average salary over the years
// alternative = withColumn(“file_name”,  input_file_name()
val years = 15 to 22
for (y <- years) {
     val players = {
          spark.read
          .option("header", true)
          .option("inferSchema", true)
          .csv(s"$archivePath/players_$y.csv")
     }

     players.agg(avg("wage_eur").as(s"avg_wage_$y")).show()
}

/*
+------------------+
|       avg_wage_15|
+------------------+
|13252.513194269917|
+------------------+

+------------------+
|       avg_wage_16|
+------------------+
|16221.539456662354|
+------------------+

+------------------+
|       avg_wage_17|
+------------------+
|11505.123186737279|
+------------------+

+------------------+
|       avg_wage_18|
+------------------+
|11393.018399367875|
+------------------+

+-----------------+
|      avg_wage_19|
+-----------------+
|9835.527421432973|
+-----------------+

+-----------------+
|      avg_wage_20|
+-----------------+
|9745.598311681193|
+-----------------+

+----------------+
|     avg_wage_21|
+----------------+
|9148.48282493723|
+----------------+

+-----------------+
|      avg_wage_22|
+-----------------+
|9017.989362811555|
+-----------------+
*/






// 8. total income per country

{
     players22
     .select("nationality_name", "wage_eur")
     .groupBy("nationality_name")
     .agg(
          sum("wage_eur").as("total_wage")
     )
     .orderBy(desc("total_wage"))
     .show()
}

/*
+----------------+----------+                                                   
|nationality_name|total_wage|
+----------------+----------+
|         England|1.765485E7|
|           Spain|1.585475E7|
|          Brazil| 1.45908E7|
|          France| 1.29011E7|
|         Germany| 1.08154E7|
|       Argentina| 8895250.0|
|           Italy| 6964050.0|
|        Portugal| 5209550.0|
|     Netherlands| 5063800.0|
|         Belgium| 4548550.0|
|          Mexico| 3709750.0|
|          Turkey| 2840200.0|
|         Denmark| 2669100.0|
|         Austria| 2607750.0|
|         Uruguay| 2372800.0|
|        Colombia| 2279100.0|
|        Scotland| 2129650.0|
|         Croatia| 2050900.0|
|           Japan| 1980000.0|
|          Poland| 1888550.0|
+----------------+----------+
*/




// 9. total value per club


{
     players22
     .select("club_name", "value_eur")
     .groupBy("club_name")
     .agg(
          sum("value_eur").as("total_team_value")
     )
     .orderBy(desc("total_team_value"))
     .show()
}


/*
+-------------------+----------------+                                          
|          club_name|total_team_value|
+-------------------+----------------+
|    Manchester City|       1.29951E9|
|Paris Saint-Germain|        1.2274E9|
|          Liverpool|      1.046375E9|
|  Manchester United|       1.00873E9|
|     Real Madrid CF|       9.87575E8|
|  FC Bayern München|       9.69475E8|
| Atlético de Madrid|       9.52475E8|
|            Chelsea|        9.3415E8|
|       FC Barcelona|         8.343E8|
|           Juventus|         7.749E8|
|  Tottenham Hotspur|         7.296E8|
|              Inter|        6.4735E8|
|  Borussia Dortmund|       6.46775E8|
|         Sevilla FC|         5.641E8|
|     Leicester City|       5.43385E8|
|         RB Leipzig|        5.2521E8|
|           AC Milan|       5.13075E8|
|             Napoli|         5.124E8|
|      Villarreal CF|         4.995E8|
|            Arsenal|         4.827E8|
+-------------------+----------------+
*/












// 10. players that earn more than 500000/year == 10000/week


{
     players22
     .select($"long_name", $"wage_eur".as("weekly_wage_eur"))
     .where(col("wage_eur") > 100000)
     .count()
}

// 156



// their average salary?

{
     players22
     .select($"long_name", $"wage_eur".as("weekly_wage_eur"))
     .where(col("weekly_wage_eur") > 100000)
     .agg(avg("weekly_wage_eur").as("top_avg_weekly_wage_eur"))
     .show()
}


/*
+-----------------------+
|top_avg_weekly_wage_eur|
+-----------------------+
|      160160.2564102564|
+-----------------------+
*/


// among the 150 top-paid players, the average salary is around 160k/week === 8M/year (wow)



// among them the french ones?

{
     players22
     .where($"nationality_name" === "France")
     .select($"long_name", $"wage_eur".as("weekly_wage_eur"))
     .where(col("weekly_wage_eur") > 100000)
     .show()
}

/*
+--------------------+---------------+
|           long_name|weekly_wage_eur|
+--------------------+---------------+
|Kylian Mbappé Lottin|       230000.0|
|        N'Golo Kanté|       230000.0|
|       Karim Benzema|       350000.0|
|         Hugo Lloris|       125000.0|
|          Paul Pogba|       220000.0|
|      Raphaël Varane|       180000.0|
|Kingsley Junior C...|       120000.0|
|   Antoine Griezmann|       220000.0|
|         Lucas Digne|       110000.0|
|       Ferland Mendy|       170000.0|
|     Ousmane Dembélé|       165000.0|
| Alexandre Lacazette|       110000.0|
|Clément Nicolas L...|       145000.0|
|     Anthony Martial|       130000.0|
|  Samuel Yves Umtiti|       125000.0|
|      Benjamin Mendy|       105000.0|
+--------------------+---------------+
*/


{
     players22
     .where($"nationality_name" === "France")
     .select($"long_name", $"wage_eur".as("weekly_wage_eur"))
     .where(col("weekly_wage_eur") > 100000)
     .agg(avg("weekly_wage_eur").as("top_avg_weekly_wage_eur"))
     .show()
}

/*
+-----------------------+                                                       
|top_avg_weekly_wage_eur|
+-----------------------+
|               170937.5|
+-----------------------+
*/

// the average salary for top-paid players matches with the global average but is a bit higher (5%)
























