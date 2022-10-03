package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, collect_set, count, explode, first, max, regexp_replace, split, to_date, when}


object Main extends App{


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  val spark = sql.SparkSession.builder()
    .master("local[1]")
    .appName("Spark2ChallengeXpandIT")
    .getOrCreate()

  val gps_user_reviews_path = "googleplaystore/googleplaystore_user_reviews.csv"
  val gps_path = "googleplaystore/googleplaystore.csv"


  val user_reviews_df = spark.read.option("header", value = true).csv(gps_user_reviews_path)
  val gps_df = spark.read.option("header", value = true).csv(gps_path)

  def part1(): DataFrame = {
    var df_1 = user_reviews_df.select("App", "Sentiment_Polarity")

    // Filter out rows with any nan value
    df_1 = df_1.na.drop()
    df_1 = df_1.filter(df_1("Sentiment_Polarity") =!= "nan")

    // Filter out non numeric strings
    df_1 = df_1.filter(df_1("Sentiment_Polarity").rlike("-?[0-9]+(\\.[0-9]+)?"))

    // Cast string to doubles
    df_1 = df_1.withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("float"))

    // Average Sentiment Polarity grouped by app
    df_1 = df_1.groupBy("App").avg("Sentiment_Polarity")

    // Column Rename
    df_1 = df_1.withColumnRenamed("avg(Sentiment_Polarity)", "Average_Sentiment_Polarity")



    df_1.printSchema()
    df_1.show()

    return df_1
  }

  def part2(): Unit ={
    var df_2 = gps_df
    //drop rows with NAN values
    df_2 = df_2.na.drop()

    df_2 = df_2.withColumn("Rating", col("Rating").cast("float"))

    //filter out rows with rating values less than 4.0 then order by rating
    df_2 = df_2.filter(df_2("Rating") >= 4.0)
    df_2 = df_2.orderBy(col("Rating").desc)

    //write to csv file with § as delimiter
    df_2.write
      .option("header", value = true)
      .option("delimiter","§")
      .csv("best_apps.csv")

    df_2.printSchema()
    df_2.show()
  }

  def part3(): DataFrame={
    var df_3 = gps_df

    //drop rows with NAN values
    df_3 = df_3.na.drop()

    //Initial Transformations to data
    df_3 = df_3.withColumn("Genres", split(col("Genres"),";"))

      .withColumn("Rating", col("Rating").cast("float"))
      .withColumn("Reviews", col("Reviews").cast("long"))
      //remove $ char from price to cast from string to numeric value
      .withColumn("Price", regexp_replace(col("Price"), "\\$","").cast("float").multiply(0.9))
      .withColumn("Size",
        when(col("Size").rlike("k|K"), //when k in string remove it to cast to float and divide by 1024
          regexp_replace(col("Size"),"k|K","").cast("float").divide(1024))
          //removes all alpha char to cast from string to float
          .otherwise(regexp_replace(col("Size"),"[a-z]|[A-Z]","").cast("float")))
      .withColumn("Last Updated",to_date(col("Last Updated"), "MMMM dd, yyyy"))


    //max() for numeric and date types, first() for strings
    df_3 = df_3.groupBy("App").agg(
      //Set instead of list to remove duplicate categories
      collect_set("Category").alias("Categories"),
      max("Rating").alias("Rating"),
      max("Reviews").alias("Reviews"),
      max("Size").alias("Size"),
      first("Installs").alias("Installs"),
      first("Type").alias("Type"),
      max("Price").alias("Price"),
      first("Content Rating").alias("Content_Rating"),
      first("Genres").alias("Genres"),
      max("Last Updated").alias("Last_Updated"),
      first("Current Ver").alias("Current_Version"),
      first("Android Ver").alias("Minimun_Android_Version"))


    df_3.printSchema()
    df_3.show()
    return df_3
  }

  def part4(): Unit = {
    val df_1 = part1()
    val df_3 = part3()

    // Join dataframes by using column App as key
    val df_4 = df_3.join(df_1, Seq("App"))

    // Write to parquet with gzip compression
    df_4.write
      .option("compression", "gzip")
      .parquet("googleplaystore_cleaned")

    df_4.printSchema()
    df_4.show()

  }

  def part5(): DataFrame ={
    val df_1 = part1()
    val df_3 = part3()

    // Join dataframes by using column App as key
    var df_4 = df_3.join(df_1, Seq("App"))

    // takes list inside column genre and makes new rows with each element of the list
    df_4 = df_4.withColumn("Genres", explode(col("Genres")))

    // groups by genre, counts each one and computes average of ratings and sentiment polarity
    df_4 = df_4.groupBy("Genres").agg(
      count("Genres").alias("Count"),
      avg("Rating").alias("Average_Rating"),
      avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
    )

    // Write to parquet with gzip compression
    df_4.write
      .option("compression", "gzip")
      .parquet("googleplaystore_metrics")

    df_4.printSchema()
    df_4.show()

    return df_4
  }

  println("Part 1")
  var df_1 = part1()

  println("Part 2")
  var df_2 = part2()

  println("Part 3")
  var df_3 = part3()

  println("Part 4")
  part4()

  println("Part5")
  var df_4 = part5()
}
