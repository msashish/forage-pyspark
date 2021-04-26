from com.crealytics.spark.excel import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("excel-email-pipeline").getOrCreate()

df = spark.read.format("com.crealytics.spark.excel").option("Header", "true").option("inferSchema", "true").load("/Users/sheelava/msashishgit/forage/input/ANZ_synthesised_transaction_dataset.xlsx")
# class 'pyspark.sql.dataframe.DataFrame'
df.show(2, truncate=False)
print(df.printSchema())

# print("Testing psv method of reading")
# df2 = spark.read.load("/Users/sheelava/msashishgit/forage/input/ANZ_synthesised_transaction_dataset.xlsx", format="csv", sep="|", inferSchema="true", header="true")
# df2 = spark.read.csv("/Users/sheelava/msashishgit/forage/input/ANZ_synthesised_transaction_dataset.xlsx", sep="|", header="true")
# df2.show(2)