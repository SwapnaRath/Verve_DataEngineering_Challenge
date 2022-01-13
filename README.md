Run the scala program in a spark shell using
spark-shell -i CalculateMetricsAndRecommendations.scala

Provide arguments and result files path on the spark prompt e.g.
scala> CalculateMetricsAndRecommendations.main(Array("./impressions.json", "./clicks.json", "./metrics.json", "./recommendations.json"))

metrics.json will contain the Metrics for the dimensions.

recommendations.json will contain the recommended advertiser_id

