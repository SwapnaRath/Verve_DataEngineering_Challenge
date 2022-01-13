import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object CalculateMetricsAndRecommendations {
    def main(args : Array[String]) : Unit = {
        val spark:SparkSession = SparkSession.builder()
           .master("local[1]").appName("Verve")
           .getOrCreate()
        val dfImpr = spark.read.option("multiline", "true").json(args(0))
        val dfClick = spark.read.option("multiline", "true").json(args(1))
        val dfImprClick = dfImpr.join(dfClick, dfImpr("id") === dfClick("impression_id"))
        val dfMetrics = dfImprClick.groupBy("app_id", "country_code").agg(count("id").as("impressions"), countDistinct("revenue").as("clicks"), sum("revenue").as("revenue_sum"))
        dfMetrics.filter("country_code is not null").sort(col("app_id").cast("int")).write.json(args(2))
        var dfPerformance = dfImprClick.groupBy("app_id", "country_code", "advertiser_id").agg(count("id").as("impressions"), countDistinct("revenue").as("clicks"), sum("revenue").as("revenue_sum"), (sum("revenue")/count("id")).as("performance"))
        var dfRecommendation = dfPerformance.groupBy("app_id", "country_code", "advertiser_id").agg(max("performance"))
        var dfOutput = dfRecommendation.sort("app_id").filter("country_code is not null").select(col("app_id"), col("country_code"), col("advertiser_id").alias("recommended_advertiser_id"))
        dfOutput.write.json(args(3))
    }
}
