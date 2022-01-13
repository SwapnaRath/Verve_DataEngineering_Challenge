# Verve Coding Challenge
Run the steps below to create json output

## 1. Run the scala program in a spark shell using
```
spark-shell -i CalculateMetricsAndRecommendations.scala
```

## 2. Provide arguments and result files path on the spark prompt e.g.
```
scala> CalculateMetricsAndRecommendations.main(Array("./impressions.json", "./clicks.json", "./metrics.json", "./recommendations.json"))
```
## Output
metrics.json will contain the Metrics for the dimensions.

recommendations.json will contain the recommended advertiser_id

