from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo1.main import wordcount
from pyspark.sql import Row


class TestMain(unittest.TestCase):
    def test_wordcount(self):
        input = spark.createDataFrame(
            [
                Row(text='bonjour je suis un test unitaire'),
                Row(text='bonjour suis test')
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(word='bonjour', count=2),
                Row(word='je', count=1),
                Row(word='suis', count=2),
                Row(word='un', count=1),
                Row(word='test', count=2),
                Row(word='unitaire', count=1),
            ]
        )

        actual = wordcount(input, 'text')

        self.assertCountEqual(actual.collect(), expected.collect())

    def test_wordcount_empty(self):
        empty_df = spark.createDataFrame([], 'text string')

        result = wordcount(empty_df, 'text')

        self.assertEqual(result.count(), 0)

    def test_wordcount_error(self):
        input_df = spark.createDataFrame([Row(text='test exeption')])

        with self.assertRaises(Exception):
            wordcount(input_df, 'invalid_column')

