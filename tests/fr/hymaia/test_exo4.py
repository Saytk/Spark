import unittest
from tests.fr.hymaia.spark_test_case import spark
from src.fr.hymaia.exo4.python_udf import category_name
from src.fr.hymaia.exo4.scala_udf import addCategoryName
from pyspark.sql.functions import udf, when, col
from pyspark.sql import Row
from pyspark.sql.types import StringType

class SparkTest(unittest.TestCase):

    def test_no_udf_operations(self):
        data = [("0", "2019-02-17", 6, 40.0), ("1", "2015-10-01", 4, 69.0)]
        df = spark.createDataFrame(data, ["id", "date", "category", "price"])
        
        df_with_category_name = df.withColumn("category_name", when(df["category"] < 6, "food").otherwise("furniture"))
        results = df_with_category_name.collect()

        self.assertEqual(results[0]['category_name'], 'furniture')
        self.assertEqual(results[1]['category_name'], 'food')
    
    def test_scala_udf(self):


        data = [("0", "2019-02-17", 6, 40.0), ("1", "2015-10-01", 4, 69.0)]
        df = spark.createDataFrame(data, ["id", "date", "category", "price"])

        df_with_category_name = df.withColumn("category_name", addCategoryName(df["category"], spark))
        results = df_with_category_name.collect()

        self.assertEqual(results[0]['category_name'], 'furniture')
        self.assertEqual(results[1]['category_name'], 'food')
    
    def test_python_udf(self):
        category_name_udf = udf(category_name, StringType())
        data = [("0", "2019-02-17", 6, 40.0), ("1", "2015-10-01", 4, 69.0)]
        df = spark.createDataFrame(data, ["id", "date", "category", "price"])
        df_with_category_name = df.withColumn("category_name", category_name_udf(df["category"]))
        results = df_with_category_name.collect()

        self.assertEqual(results[0]['category_name'], 'furniture')
        self.assertEqual(results[1]['category_name'], 'food')
    
    def test_integration(self):
        test_data = [
            Row(id=1, date="2022-01-01", category=6, price=40.0, category_name="furniture"),
            Row(id=2, date="2022-01-02", category=4, price=69.0, category_name="food"),
            Row(id=3, date="2022-01-03", category=7, price=55.0, category_name="furniture"),
            Row(id=4, date="2022-01-04", category=3, price=15.0, category_name="food"),
            Row(id=5, date="2022-01-05", category=8, price=95.0, category_name="furniture")
        ]

        df = spark.createDataFrame(test_data)

        category_name_udf = udf(lambda category: 'food' if category < 6 else 'furniture', StringType())
        df_python_udf = df.withColumn('category_name', category_name_udf(col('category')))

        df_no_udf = df.withColumn('category_name', when(col('category') < 6, 'food').otherwise('furniture'))

        df_scala_udf = df.withColumn('category_name', addCategoryName(col('category'), spark))

        self.assertEqual(df_python_udf.count(), df_no_udf.count())
        self.assertEqual(df_no_udf.count(), df_scala_udf.count())

        self.assertTrue(df_python_udf.filter(col('category_name') == 'food').count() > 0)
        self.assertTrue(df_no_udf.filter(col('category_name') == 'furniture').count() > 0)
        self.assertTrue(df_scala_udf.filter(col('category_name') == 'food').count() > 0)




