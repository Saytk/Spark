from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import time

def category_name(category):
    return 'food' if category < 6 else 'furniture'

def main():
    spark = SparkSession.builder.appName("Python UDF Example").getOrCreate()
    category_name_udf = udf(category_name, StringType())
    df = spark.read.csv('../../../resources/exo4/sell.csv', header=True, inferSchema=True)
    start_time = time.time()

    df_with_category_name = df.withColumn('category_name', category_name_udf(df['category']))
    df_with_category_name.write.mode('overwrite').csv('/tmp/python_udf_output')

    end_time = time.time()
    print(f"Execution time with Python UDF: {end_time - start_time} seconds")

if __name__ == "__main__":
    main()
