val movies_df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("s3://kai-airflow-storage/movies.csv")

movies_df.write.mode("overwrite").parquet("s3://kai-airflow-storage/movielens-parquet/movies/")
