from pyspark.sql.dataframe import DataFrame

def display(dataset: DataFrame) -> None:
  dataset.show()
