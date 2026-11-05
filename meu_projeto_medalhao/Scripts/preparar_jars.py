import os
import sys
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Ajustes específicos para estabilidade da rede no Windows
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

PACKAGES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "org.postgresql:postgresql:42.7.4",
]

spark = (SparkSession.builder
    .master("local[1]")
    .appName("preparar-jars")
    .config("spark.jars.packages", ",".join(PACKAGES))
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.port", "0")
    .config("spark.network.timeout", "600s")
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.ui.enabled", "false")
    .getOrCreate())

# Oculta logs informativos durante o desligamento
spark.sparkContext.setLogLevel("ERROR")

print("Jars baixados e prontos. Spark", spark.version)

# Garante a liberação limpa da JVM
spark.stop()