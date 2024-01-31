val genome_scores_df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("s3://kai-airflow-storage/genome-scores.csv")

genome_scores_df.write.mode("overwrite").parquet("s3://kai-airflow-storage/movielens-parquet/genome-scores/")
