import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from src.fr.hymaia.exo2.spark_aggregate_job import calculate_population_by_department
from src.fr.hymaia.exo2.spark_clean_job import read_data, filter_adults, join_clients_cities, add_department_column

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    glueContext = GlueContext(spark.sparkContext)
    job = Job(glueContext)
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    job.init(args['JOB_NAME'], args)

    input_clients_path = "s3://bucketsparkexam/Data/city_zipcode.csv"
    input_cities_path = "s3://bucketsparkexam/Data/clients_bdd.csv"
    output_clean_path = "s3://bucketsparkexam/Output/clean/"
    output_aggregate_path = "s3://bucketsparkexam/Output/aggregate/"

    df_clients = read_data(spark, input_clients_path)
    df_cities = read_data(spark, input_cities_path)
    df_clients_adults = filter_adults(df_clients)
    df_joined = join_clients_cities(df_clients_adults, df_cities)
    df_with_department = add_department_column(df_joined)
    df_with_department.write.parquet(output_clean_path)

    df_clean = spark.read.parquet(output_clean_path)
    df_population = calculate_population_by_department(df_clean)
    df_population.coalesce(1).write.option("header", "true").csv(output_aggregate_path)

    job.commit()
