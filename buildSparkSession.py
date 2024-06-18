import json
from pyspark.sql import SparkSession


def initialize_spark_session():

    # Load configuration from JSON file
    with open('config.json', 'r') as f:
        config = json.load(f)

    mysql_user = config["MYSQL_USER"]
    mysql_password = config["MYSQL_PASSWORD"]
    mysql_host = config["MYSQL_HOST"]
    mysql_port = config["MYSQL_PORT"]

    spark = SparkSession.builder \
        .appName("Config Table Task") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    connection_properties = {"user": mysql_user, "password": mysql_password, "driver": "com.mysql.cj.jdbc.Driver"}

    return spark, mysql_user, mysql_password, mysql_host, mysql_port, connection_properties




initialize_spark_session()
