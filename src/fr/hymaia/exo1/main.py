import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()

def main():
    spark = SparkSession.builder \
                        .master("local[*]") \
                        .appName("wordcount") \
                        .getOrCreate()

    df = spark.read.csv('~/spark/spark-handson/src/resources/exo1/data.csv', header=True)

    result = wordcount(df, 'text')  

    result.write.mode("overwrite").partitionBy("count").parquet("data/exo1/output")

    spark.stop()

if __name__ == "__main__":
    main()