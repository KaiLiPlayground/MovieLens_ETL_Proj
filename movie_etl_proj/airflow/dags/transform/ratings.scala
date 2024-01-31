val ratings_df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("s3://kai-airflow-storage/ratings.csv")

ratings_df.write.mode("overwrite").parquet("s3://kai-airflow-storage/movielens-parquet/ratings/")
