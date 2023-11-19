from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("SimpleApp")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .master("local[4]")
        .getOrCreate()
    )
    return spark

print('__init__')