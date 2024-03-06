from tests.fr.hymaia.spark_test_case import spark
import unittest
from pyspark.sql import Row
from src.fr.hymaia.exo2.spark_clean_job import filter_age, join, add_department
from src.fr.hymaia.exo2.spark_aggregate_job import calculate
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class SparkTest(unittest.TestCase):
        
    def test_filter_age(self):
        test_data = [Row(name="Jojo", age=17, zip="12345"), Row(name="Bob", age=21, zip="23456")]
        df = spark.createDataFrame(test_data)

        result = filter_age(df)

        self.assertEqual(result.count(), 1)
        self.assertEqual(result.collect()[0].name, "Bob")

    def test_join(self):
        clients_data = [Row(name="Jojo", age=30, zip="12345")]
        cities_data = [Row(zip="12345", city="Test")]
        df_clients = spark.createDataFrame(clients_data)
        df_cities = spark.createDataFrame(cities_data)

        result = join(df_clients, df_cities)

        expected = [Row(name="Jojo", age=30, zip="12345", city="Test")]
        self.assertEqual(result.collect(), expected)

    def test_add_department(self):
        data = [Row(zip="20190"), Row(zip="20200"), Row(zip="05001")]
        df = spark.createDataFrame(data)

        result = add_department(df)

        expected = [Row(zip="20190", departement="2A"), Row(zip="20200", departement="2B"), Row(zip="05001", departement="05")]
        expected_df = spark.createDataFrame(expected)
        result_sorted = result.sort('zip').collect()
        expected_sorted = expected_df.sort('zip').collect()
        self.assertEqual(result_sorted, expected_sorted)

    def test_calculate(self):
        test_data = [Row(departement="75", name="Jojo"), Row(departement="75", name="Bob"), Row(departement="01", name="Charlie")]
        df = spark.createDataFrame(test_data)

        result = calculate(df)

        expected_counts = {"75": 2, "01": 1}
        for row in result.collect():
            self.assertEqual(row["nb_people"], expected_counts[row["departement"]])

    def test_calculate__empty(self):
        empty_df = spark.createDataFrame([], 'departement string')

        result = calculate(empty_df)

        self.assertEqual(result.count(), 0)

    def test_integration(self):
        schema_clients = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("zip", StringType(), True)
        ])

        schema_cities = StructType([
            StructField("zip", StringType(), True),
            StructField("city", StringType(), True)
        ])

        df_clients_test = spark.createDataFrame([
            ("Turing", 21, "10001"),
            ("Gates", 12, "10001"),
            ("Hopper", 42, "90001"),
            ("test", 32, "90001")
        ], schema_clients)

        df_cities_test = spark.createDataFrame([
            ("10001", "New York"),
            ("90001", "Los Angeles")
        ], schema_cities)

        df_clients_adults = filter_age(df_clients_test)
        df_joined = join(df_clients_adults, df_cities_test)
        df_with_department = add_department(df_joined)
        df_population = calculate(df_with_department)

        actual_with_department = sorted(df_with_department.collect(), key=lambda row: (row['zip'], row['name']))
        expected_with_department = sorted([
            ("Turing", 21, "10001", "New York", "10"),
            ("Hopper", 42, "90001", "Los Angeles", "90"),
            ("test", 32, "90001", "Los Angeles", "90")
        ], key=lambda x: (x[2], x[0]))

        self.assertEqual(actual_with_department, expected_with_department)

        actual_population = {row['departement']: row['nb_people'] for row in df_population.collect()}
        expected_population = {"10": 1, "90": 2} 

        self.assertEqual(actual_population, expected_population)

                

   



    
