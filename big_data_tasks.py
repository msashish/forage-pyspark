import argparse
from bigdata.forage_tasks import SparkTask
from bigdata.utilities import timeme, write_df_to_csv, write_rdd_to_csv


def get_arguments() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        prog="python big_data_tasks.py",
        usage="%(prog)s [-h] [--file]",
    )

    parser.add_argument(
        "--file", "-f",
        dest='file_path',
        type=str
    )
    return parser.parse_args()


@timeme
def task_using_df(spark_session):
    processed_df = spark_session.process_df()
    write_df_to_csv(processed_df, "/Users/sheelava/msashishgit/forage/output/anz_synth_df.csv")


@timeme
def task_using_rdd(spark_session):
    processed_rdd = spark_session.process_rdd()
    write_rdd_to_csv(processed_rdd, "/Users/sheelava/msashishgit/forage/output_rdd/anz_synth_rdd.csv")


args = get_arguments()
task_session = SparkTask(args.file_path)
task_using_df(task_session)
task_using_rdd(task_session)
