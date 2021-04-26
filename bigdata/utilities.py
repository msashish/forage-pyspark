import pyspark.sql.functions as func
from pyspark.sql import Row
import pathlib
import shutil
from time import perf_counter


def split_df_column(df, column_name, new_column_name1, new_column_name2, delimiter=' '):
    split_columns = func.split(df[column_name], delimiter)
    return df.withColumn(new_column_name1, split_columns.getItem(0)) \
        .withColumn(new_column_name2, split_columns.getItem(1)) \
        .drop(column_name)


def write_df_to_csv(df, output_file_path):
    df.repartition(1).write.format('com.databricks.spark.csv') \
        .mode('overwrite') \
        .option("delimiter", ',') \
        .save(output_file_path, header='true')


def write_rdd_to_csv(rdd, output_file_path):
    if pathlib.Path(output_file_path).exists():
        shutil.rmtree(output_file_path)
    rdd.saveAsTextFile(output_file_path)


def timeme(function):
    def timed(*args, **kwargs):
        start = perf_counter()
        function(*args, **kwargs)
        end = perf_counter()
        print(f"    Processing time taken by function {function.__name__} is = {end-start : .15f} seconds")
    return timed
