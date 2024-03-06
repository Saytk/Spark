import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
                        .appName("ClientCityJoin") \
                        .getOrCreate()

    # Lire les fichiers
    df_clients = spark.read.csv('../../../resources/exo2/clients.csv', header=True, inferSchema=True)
    df_villes = spark.read.csv('../../../resources/exo2/villes.csv', header=True, inferSchema=True)

    # Filtrer les clients majeurs
    df_majeurs = df_clients.filter(col('age') >= 18)

    # Joindre les tables clients et villes
    df_joined = df_majeurs.join(df_villes, 'zip')

    # Ajouter la colonne département
    df_joined = df_joined.withColumn('departement', 
                                     when(col('zip') <= '20190', '2A')
                                     .when(col('zip') > '20190', '2B')
                                     .otherwise(col('zip').substr(1, 2)))

    # Calcul de la population par département
    df_population = df_joined.groupBy('departement').count().withColumnRenamed('count', 'nb_people')
    df_population = df_population.orderBy(col('nb_people').desc(), col('departement'))

    # Écrire le résultat dans un fichier Parquet
    df_joined.write.parquet('data/exo2/output')

    # Écrire le résultat dans un fichier CSV
    df_population.coalesce(1).write.csv('data/exo2/aggregate', header=True)

    spark.stop()

if __name__ == "__main__":
    main()
