import pyspark
from pyspark.sql import *
import os
os.environ["java.io.tmpdir"] = "C:\\filerun"
scala_version = '2.12'  # TODO: Ensure this is correct
spark_version = pyspark.__version__

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.5.1'
]

args = os.environ.get('PYSPARK_SUBMIT_ARGS', '')
if not args:
    args = f'--packages {",".join(packages)}'
    print('Using packages', packages)
    os.environ['PYSPARK_SUBMIT_ARGS'] = f'{args} pyspark-shell'
else:
    print(f'Found existing args: {args}')
# Step 1: Create a SparkSession
spark = SparkSession.builder \
    .appName("SQLServerToKafka") \
    .config("spark.driver.extraClassPath",
            "C:\\Users\\chait\\Downloads\\sqljdbc_12.4.1.0_enu\\sqljdbc_12.4\\enu\\jars\\mssql-jdbc-12.4.1.jre8.jar") \
    .config("spark.executor.extraClassPath",
            "C:\\Users\\chait\\Downloads\\sqljdbc_12.4.1.0_enu\\sqljdbc_12.4\\enu\\jars\\mssql-jdbc-12.4.1.jre8.jar") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g").getOrCreate()
#spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
# Step 3: Read data from SQL Server
jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=prac;"
properties = {
    "user": "chaithanya",
    "password": "12345678",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "encrypt": "false"
}

# Corrected code: Specify the table name, not the full SQL query
table_name = "emp_manager"
sql_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)
sql_df.cache()

kafka_df = sql_df.selectExpr("CAST( emp_id AS STRING) AS key", "to_json(struct(*)) AS value")
kafka_df.show()
kafka_df \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "PLAINTEXT://localhost:9092") \
    .option("topic", "quickstart-events3") \
    .outputMode("complete")\
    .option("checkpointlocation","chk-point-dir")\
    .save()
print("Data written to Kafka topic 'quickstart-events3'")

