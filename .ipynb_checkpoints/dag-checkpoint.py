import os
import pandas as pd
import polars as pl
from dagstermill import define_dagstermill_asset
from dagstermill import ConfigurableLocalOutputNotebookIOManager
from dagster import (
    AssetIn,
    Field,
    Int,
    asset,
    file_relative_path,
    Definitions,
    Config,
    OpExecutionContext,
    ConfigurableResource,
    AssetIn,
    define_asset_job,
    FilesystemIOManager,
    build_schedule_from_partitioned_job,
    op,
    job,
    sensor,
    RunRequest,
    RunConfig,
    EnvVar,
    ConfigurableIOManager
)

from datetime import datetime

from dagster import DailyPartitionsDefinition
from dagster import make_values_resource
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

import assets

m_partitions = DailyPartitionsDefinition(start_date=datetime(2023, 11, 15))


partition_bounds = make_values_resource(start=str, end=str)


class MyConfig(Config):
    param_1: str = ''


@asset(
        group_name="template_tutorial",
    )
def param_asset(context: OpExecutionContext, config: MyConfig) -> str:
    context.log.info(config.param_1)
    return config.param_1


param_asset_job = define_asset_job(
    name="param_asset_job",
    selection="param_asset"
    )


class MyResources(ConfigurableResource):
    a_str: str
    env_type: str
    config_path: str


#@asset(
#    ins={
#        "first_asset": AssetIn(input_manager_key="pandas_series"),
#        "second_asset": AssetIn(input_manager_key="pandas_series"),
#    }
#)


class LocalParquetIOManager(ConfigurableIOManager):
    def _get_path(self, context: OpExecutionContext):
        print(dir(context))
        print(context.asset_partition_key)
        print(context.asset_key.path)
        return os.path.join(*context.asset_key.path,
                            context.asset_partition_key
                            )

    def handle_output(self, context, obj: DataFrame):
        obj.write.mode("overwrite").parquet(self._get_path(context))

    def load_input(self, context):
        path = self._get_path(context.upstream_output)
        return pl.read_parquet(f'{path}/*.parquet').to_pandas()
        #return self._get_path(context.upstream_output)


# per asset io manager
# @asset(io_manager_key="s3_io_manager")
@asset(group_name="template_tutorial",
       partitions_def=m_partitions,
       io_manager_key="prqt_io_manager",
       ins={
           "param": AssetIn("param_asset"),
            },
       )
def people(context: OpExecutionContext, param: str) -> DataFrame:
    start, end = context.asset_partitions_time_window_for_output()
    context.log.info(param)
    spark = assets.get_spark_session()
    context.log.info(spark.sparkContext._conf.get("spark.app.id"))
    import time
    time.sleep(60)
    schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
    rows = [Row(name="Thom", age=51), Row(name="Jonny", age=48), Row(name="Nigel", age=49)]
    return spark.createDataFrame(rows, schema)


@asset(group_name="template_tutorial",
       partitions_def=m_partitions,
       io_manager_key="prqt_io_manager",
       ins={
           "param": AssetIn("param_asset"),
            },
       )
def dogs(context: OpExecutionContext, param: str) -> DataFrame:
    start, end = context.asset_partitions_time_window_for_output()
    spark = assets.get_spark_session()
    context.log.info(spark.sparkContext._conf.get("spark.app.id"))
    import time
    time.sleep(30)
    schema = StructType([StructField("name", StringType()), StructField("tail", IntegerType())])
    rows = [Row(name="tuzik", age=1), Row(name="bobik", age=10), Row(name="putin", age=0)]
    return spark.createDataFrame(rows, schema)


@asset(group_name="template_tutorial",
       partitions_def=m_partitions,
       io_manager_key="prqt_io_manager",
       ins={
           "param": AssetIn("param_asset"),
            },
       )
def cats(context: OpExecutionContext, param: str) -> DataFrame:
    start, end = context.asset_partitions_time_window_for_output()
    spark = assets.get_spark_session()
    context.log.info(spark.sparkContext._conf.get("spark.app.id"))
    import time
    time.sleep(60)
    schema = StructType([StructField("name", StringType())])
    rows = [Row(name="manya"), Row(name="kabaeva")]
    return spark.createDataFrame(rows, schema)


@asset(group_name="template_tutorial",
       partitions_def=m_partitions,
       io_manager_key='io_manager',
       ins={
           "df_people": AssetIn("people"),
           "df_dogs": AssetIn("dogs"),
           "df_cats": AssetIn("cats"),
            },
       )
def res_asset(context: OpExecutionContext,
                         df_people,
                         df_dogs,
                         df_cats
                         ) -> pd.DataFrame:
    start, end = context.asset_partitions_time_window_for_output()
    #context.log.info(df_people)
    context.log.info([start, end])
    import time
    time.sleep(15)
    context.log.info(df_people)
    context.log.info(df_dogs)
    context.log.info(df_cats)
    return df_dogs
    # context.log.info(assets.spark.sparkContext._conf.get("spark.app.id"))


partitions_job = define_asset_job(
    name="partitions_job",
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 2,      # limits concurrent assets to 3
                },
            }
        }
    },
    selection=[
        "people",
        "dogs",
        "cats",
        "res_asset"

    ]
    )

do_partitioned_schedule = build_schedule_from_partitioned_job(
    partitions_job,
)


class FileConfig(Config):
    filename: str = 'trigger.txt'


@op
def process_file(context, config: FileConfig):
    context.log.info(f'log: {config.filename}')


#@job
#def log_file_job():
#    process_file()


MY_DIRECTORY = '/home/jovyan/work/data'


@sensor(job=partitions_job)
def my_directory_sensor():
    #conf = MyConfig()
    filepath = os.path.join(MY_DIRECTORY, 'trigger.txt')
    if os.path.isfile(filepath):
        yield RunRequest()


defs = Definitions(
    assets=[param_asset, people, dogs, cats, res_asset],
    resources={
        "output_notebook_io_manager": ConfigurableLocalOutputNotebookIOManager(),
        "resources": MyResources(a_str='a',
                                 env_type=EnvVar("ENV_TYPE"),
                                 config_path=os.getenv("ENV_TYPE")
                                 ),
        "io_manager": FilesystemIOManager(
            base_dir="/home/jovyan/work/data/dev/dag_assets"
            ),
        "prqt_io_manager": LocalParquetIOManager(
            base_dir="/home/jovyan/work/data/dev/dag_assets"
        )
        },
    # schedules=[do_partitioned_schedule],
    sensors=[my_directory_sensor],
    jobs=[param_asset_job, partitions_job],
)
