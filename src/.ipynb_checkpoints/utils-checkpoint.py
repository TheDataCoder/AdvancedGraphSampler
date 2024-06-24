import findspark
import pyspark.sql
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from conf.folders import PATH_PROJECT, params

def init_spark():
    """Initialize spark session.

    Returns:
        spark (SparkSession): SparkSession
    """
    findspark.init()

    ram_size = params["resources"]["spark_ram_size"]
    num_cores = params["resources"]["spark_num_cores"]
    spark = (
        SparkSession.builder.master(f"local[{num_cores}]")
        .config("spark.local.dir", str(PATH_PROJECT / "spark_tmp"))
        .config("spark.driver.memory", f"{ram_size}g")
        .config("spark.driver.maxResultSize", f"{ram_size}g")
        .config("spark.storage.memoryFraction", "1")
        # Required for large column names.
        .config("spark.sql.debug.maxToStringFields", "2000")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.port", "8082")
        # To opimize toPandas speed
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    return spark