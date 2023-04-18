package com.verve.assignment.usecase

import com.verve.assignment.configs.ApplicationConfig
import com.verve.assignment.domain.Clicks
import com.verve.assignment.domain.Impressions
import com.verve.assignment.module.Spark
import com.verve.assignment.repository.EventRepository
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Part2 {
  /**
   * The general idea to recommend advertiser is to calculate performance of advertiser per app_id and per country_code
   * separately and then merge the sorted performance together and recommend the top five of the merged advertiser list
   * per app_id and country_code. For calculation of performance/score I used window function to facilitate process.
   */
  def call(): Unit = {
    val clickFilePath: String = ApplicationConfig.clickFilePath
    val impressionFilePath: String = ApplicationConfig.impressionFilePath
    val spark = Spark.session()
    val eps = 0.00001

    val impressionsEncoder = Encoders.bean(classOf[Impressions])
    val clicksEncoder = Encoders.bean(classOf[Clicks])

    //   ************************Read impression and clicks data ************************************

    val dfImpressions = spark.read.option("multiline", "true")
      .json(impressionFilePath)
      .filter(col("advertiser_id").isNotNull).na.fill("null")
      .as(impressionsEncoder)

    val dfClicks = spark.read.option("multiline", "true").json(clickFilePath).na.fill("null").as(clicksEncoder)

    //   ************************Left join impression with clicks to access revenue ************************************
    val dfImpressionWithClicks = dfImpressions
      .join(dfClicks, dfImpressions("id") === dfClicks("impression_id"), "left")
      .na.fill(0, Seq("revenue"))
      .select("advertiser_id", "app_id", "country_code", "revenue")

    //   ************************Define window functions to calculate performance per app_id and country_code
    //   separately. Here score columns saves performance ************************************

    val windowSpecAppAdv = Window.partitionBy("app_id", "advertiser_id")
    val windowSpecCtyAdv = Window.partitionBy("country_code", "advertiser_id")

    //**************************Performance calculation is achieved such that:
    // advertiser_total_revenue_per[app_id|country_code] / advertiser_total_impression_per[app_id|country_code]
    val dfScore = dfImpressionWithClicks
      .withColumn("adv_per_app_impression_count", count("*").over(windowSpecAppAdv))
      .withColumn("adv_per_app_revenue", sum("revenue").over(windowSpecAppAdv))
      .withColumn("adv_per_app_score", col("adv_per_app_revenue") / col("adv_per_app_impression_count") + eps)
      .withColumn("adv_per_cty_impression_count", count("*").over(windowSpecCtyAdv))
      .withColumn("adv_per_cty_revenue", sum("revenue").over(windowSpecCtyAdv))
      .withColumn("adv_per_cty_score", col("adv_per_cty_revenue") / col("adv_per_cty_impression_count") + eps)
      .drop("adv_per_app_impression_count", "adv_per_app_revenue", "adv_per_cty_impression_count", "adv_per_cty_revenue")
      .filter(col("adv_per_app_score") > 0 && col("adv_per_cty_score") > 0)

    //**************************Get the top 5 advertisers per app_id based on their score of that app_id sorted in descending
    // and collect them as a list in a new column named: advertiser_ids_app. Note if the list contains less than 5 element, only
    // the max number of element is considered
    val dfAppScore = dfScore
      .dropDuplicates("app_id", "advertiser_id", "adv_per_app_score")
      .orderBy(col("adv_per_app_score").desc)
      .groupBy("app_id")
      .agg(collect_list("advertiser_id").alias("advertiser_ids"), collect_list("adv_per_app_score").alias("adv_per_app_score_list"))
      .select(
        col("app_id"),
        slice(col("advertiser_ids"), 1, 5).alias("advertiser_ids_app")
      )

    //**************************Get the top 5 advertisers per country_code based on their score of that country_code sorted in descending
    //// and collect them as a list in a new column named: advertiser_ids_cty.
    val dfCtyScore = dfScore
      .dropDuplicates("country_code", "advertiser_id", "adv_per_cty_score")
      .orderBy(col("adv_per_cty_score").desc)
      .groupBy("country_code")
      .agg(collect_list("advertiser_id").alias("advertiser_ids"), collect_list("adv_per_cty_score").alias("adv_per_cty_score_list"))
      .select(
        col("country_code"),
        slice(col("advertiser_ids"), 1, 5).alias("advertiser_ids_cty")
      )

    //**************************Get unique pair of app_id and country_code to join with the previous result later
    val dfCtyApp = dfImpressionWithClicks
      .groupBy("app_id", "country_code").count()
      .select("app_id", "country_code")

    //**************************Join first per app_id to get the topest advertisers per app_id and join by country_code
    // to get the topest advertisers per country_code. Also union these two top lists and select the first five elements as recommended_advertiser_ids
    val df = dfCtyApp
      .join(dfAppScore, dfCtyApp("app_id") === dfAppScore("app_id"), "inner")
      .join(dfCtyScore, dfCtyApp("country_code") === dfCtyScore("country_code"), "inner")
      .drop(dfAppScore("app_id")).drop(dfCtyScore("country_code"))
      .withColumn("recommended_advertiser_ids", slice(array_union(col("advertiser_ids_app"), col("advertiser_ids_cty")), 1, 5))
      .select(col("app_id"), col("country_code"), col("recommended_advertiser_ids"))
      .dropDuplicates()

    //    df.agg(to_json(collect_list(struct(col("app_id"),col("country_code"), col("recommended_advertiser_ids")))))
    //      .write
    //      .mode(SaveMode.Overwrite)
    //      .text(part2FilePath)

    //**************************Collect the final result and map each object to a Map object then write list to a json file
    val json = df.collect().toList
      .map(element => Map("app_id" -> element.get(0), "country_code" -> element.get(1), "recommended_advertiser_ids" -> element.get(2)))
    EventRepository.write(2)(json)

  }
}
