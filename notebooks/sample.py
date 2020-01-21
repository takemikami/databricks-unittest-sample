# import spark-context and databricks stub / for unit testing
from stub import dbutils
from stub import display
import pyspark.sql

spark = pyspark.sql.SparkSession.builder.appName('local').getOrCreate()

# COMMAND ----------

import time
import os
from typing import Any
from pyspark.sql import DataFrame

# define utility functions
def checkpoint(process: Any, file: str) -> DataFrame:
  start = time.time()
  if os.path.exists("{}/_SUCCESS".format(file)):
    print("==| {} : skip".format(file))
    return spark.read.parquet(file)
  dataset = process()
  dataset.write.mode("overwrite").parquet(file)
  print("==> {} : write / {} s".format(file, time.time() - start))
  return spark.read.parquet(file)

# COMMAND ----------

# define main process
def main() -> None:
  files = dbutils.fs.ls("file:///tmp")
  for f in files:
    print(f.path)

  dataset = spark.createDataFrame([
      ["hoge", 'fuga'],
  ], ["id", "body"])
  display(dataset)

if __name__ == '__main__':
  # execute main
  main()
