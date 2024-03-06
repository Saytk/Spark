from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("unit test") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config("spark.jars", "src/resources/exo4/udf.jar")\
    .getOrCreate()
