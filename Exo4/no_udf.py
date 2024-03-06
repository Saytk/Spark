from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, sum
from pyspark.sql.window import Window

def main():
    spark = SparkSession.builder.appName("No UDF Example").getOrCreate()

    df = spark.read.csv('../../../resources/exo4/sell.csv', header=True, inferSchema=True)

    df_with_category_name = df.withColumn('category_name',
                                          when(col('category') < 6, 'food').otherwise('furniture'))

    windowSpecDay = Window.partitionBy('date', 'category_name')

    df_with_total_daily = df_with_category_name.withColumn('total_price_per_category_per_day',
                                                           sum('price').over(windowSpecDay))

    df_with_total_daily.show()

if __name__ == "__main__":
    main()
