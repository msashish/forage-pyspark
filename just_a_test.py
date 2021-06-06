from com.crealytics.spark.excel import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("excel-email-pipeline").getOrCreate()

df = spark.read.format("com.crealytics.spark.excel").option("Header", "true").option("inferSchema", "true").load("ANZ_synthesised_transaction_dataset.xlsx")
# class 'pyspark.sql.dataframe.DataFrame'
df.show(2, truncate=False)
print(df.printSchema())

