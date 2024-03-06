from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

def filter_age(df):
    return df.filter(col('age') >= 18)

def join(df_clients, df_cities):
    df_joined = df_clients.join(df_cities, 'zip')
    
    columns_order = ['name', 'age', 'zip', 'city']  
    
    df_reordered = df_joined.select(*columns_order)
    
    return df_reordered

def add_department(df):
    return df.withColumn('departement',
                         when(col('zip').substr(1, 2) == '20', 
                              when(col('zip') <= '20190', '2A').otherwise('2B'))
                         .otherwise(col('zip').substr(1, 2)))

def main():
    spark = SparkSession.builder.appName("CleanJob").getOrCreate()
    df_clients = spark.read.csv('../../../resources/exo2/clients_bdd.csv')
    df_cities = spark.read.csv('../../../resources/exo2/city_zipcode.csv')

    df_clients_adults = filter_age(df_clients)
    df_joined = join(df_clients_adults, df_cities)
    df_with_department = add_department(df_joined)

    df_with_department.write.parquet('data/exo2/clean',mode='overwrite')

    spark.stop()

if __name__ == "__main__":
    main()
