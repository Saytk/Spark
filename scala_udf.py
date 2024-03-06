from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
import time

def addCategoryName(col, spark):
    sc = spark.sparkContext
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

def main():
    spark = SparkSession.builder.appName("Scala UDF in Python")\
        .config('spark.jars', '../../../resources/exo4/udf.jar')\
        .getOrCreate()
    df = spark.read.csv('../../../resources/exo4/sell.csv', header=True, inferSchema=True)
    start_time = time.time()

    df_with_category_name = df.withColumn('category_name', addCategoryName(df['category'], spark))
    df_with_category_name.write.mode('overwrite').csv('/tmp/scala_udf_output')

    end_time = time.time()
    print(f"Execution time with Scala UDF: {end_time - start_time} seconds")

if __name__ == "__main__":
    main()
