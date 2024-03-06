from pyspark.sql import SparkSession

# Initialisation de la SparkSession
spark = SparkSession.builder \
    .appName("lire") \
    .getOrCreate()

# Chemin du fichier Parquet à lire
parquet_file_path = 'data/exo2/clean'

# Lecture du fichier Parquet
df = spark.read.parquet(parquet_file_path)

# Affichage des premières lignes du DataFrame
df.show()

# Arrêt de la SparkSession
spark.stop()
