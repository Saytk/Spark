from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def calculate(df):
    return df.groupBy('departement').count().withColumnRenamed('count', 'nb_people') \
             .orderBy(col('nb_people').desc(), col('departement'))

def main():
    spark = SparkSession.builder.appName("AggregateJob").getOrCreate()

    df_clean = spark.read.parquet('data/exo2/clean')

    df_population = calculate(df_clean)

    df_population.coalesce(1).write.option("header", "true").mode('overwrite').csv('data/exo2/aggregate')

    spark.stop()

if __name__ == "__main__":
    main()
