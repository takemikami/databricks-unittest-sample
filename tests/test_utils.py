import unittest
import os
from stub import dbutils
import pyspark.sql
from pyspark.sql import functions as F

from notebooks.sample import checkpoint

spark = pyspark.sql.SparkSession.builder.appName('local').getOrCreate()
_temporary_path = "file:///tmp/databricks-unittest-sample-tmp"

class UtilsTestCases(unittest.TestCase):

  # Datasetのparquetファイル出力、既に存在する場合は処理せず読込のみ
  def test_checkpoint(self):
    dataset = spark.createDataFrame([
        ["user001", 3, 5],
        ["user001", 6, 6],
        ["user001", 2, 7],
        ["user001", 1, 8],
        ["user002", 4, 9],
        ["user002", 1, 10],
        ["user002", 1, 8],
        ["user002", 5, 8],
    ], ["id", "type", "type2"])

    dbutils.fs.rm(_temporary_path, recurse=True)
    self.assertFalse(os.path.exists("{}/xyz".format(_temporary_path)[7:]))

    df = checkpoint(
        process=lambda: dataset
          .withColumn("hoge", F.col("type") * F.lit(2)),
        file="{}/xyz".format(_temporary_path))
    self.assertTrue(os.path.exists("{}/xyz".format(_temporary_path)[7:]))
    self.assertEquals(8, df.count())
