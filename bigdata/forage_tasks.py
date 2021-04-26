from pyspark.sql import SparkSession, Row
from bigdata.utilities import split_df_column


class SparkTask:

    def __init__(self, excel_file_path):
        self.excel_file_path = excel_file_path
        self.spark = SparkSession.builder.appName("forage-bigdata-task").getOrCreate()
        self.df = self.read_excel()

    def read_excel(self):
        return self.spark.read.format("com.crealytics.spark.excel")\
            .option("Header", "true").option("inferSchema", "true")\
            .load(self.excel_file_path)

    def process_df(self):
        print(f"DF processing of excel file {self.excel_file_path}")
        filtered_df = self.df.filter((self.df['status'] == "authorized") & (self.df['card_present_flag'] == 0))
        long_lat_split_df = split_df_column(
            filtered_df,
            "long_lat",
            "long",
            "lat"
        )
        merchant_long_lat_split_df = split_df_column(
            long_lat_split_df,
            "merchant_long_lat",
            "merchant_long",
            "merchant_lat"
        )
        print(f"    Input data rows: before processing: {self.df.count()} "
              f"and after processing: {merchant_long_lat_split_df.count()}")

        return merchant_long_lat_split_df

    def process_rdd(self):
        print(f"\nRDD processing of excel file {self.excel_file_path}")
        rdd = self.df.rdd

        filtered_rdd = rdd.filter(lambda row: row.status == "authorized")\
            .filter(lambda row: row.card_present_flag == 0)

        long_lat_split_rdd = filtered_rdd.map(
            lambda k: Row(**k.asDict(),
                          long=float(k.long_lat.split(' ')[0]),
                          lat=float(k.long_lat.split(' ')[1])))

        merchant_long_lat_split_rdd = long_lat_split_rdd.map(
            lambda k: Row(**k.asDict(),
                          merch_long=float(k.merchant_long_lat.split(' ')[0]),
                          merch_lat=float(k.merchant_long_lat.split(' ')[1])))

        print(f"    Input data rows: before processing: {rdd.count()} "
              f"and after processing: {merchant_long_lat_split_rdd.count()}")

        return merchant_long_lat_split_rdd
