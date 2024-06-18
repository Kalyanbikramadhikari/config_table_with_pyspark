from buildSparkSession import initialize_spark_session
from pyspark.sql.types import StructField,DateType, StructType, IntegerType, StringType
from uploadIntoMYSql import uploadIntoMySql
import datetime


spark,*_ = initialize_spark_session()


def config_table_two():
    schema = StructType([
        StructField("config_table_id", IntegerType(), nullable=False),
        StructField("sourcefield", StringType(), nullable=False),
        StructField("destinationfield", StringType(), nullable=False),

    ])

    data_config = [
        (1, "account_number", "account_no"),

        (1, "customer_code", "code_of_customer"),
        (1, "product", "product_name"),
        (1, "product_category", "product_category"),

        # (2, "ï»¿tran_date", "transaction_date"),
        (2, "account_number", "account_no"),
        (2, "lcy_amount", "transaction_amount"),
        (2, "description1", "description"),

        (3, "Date", "transaction_date"),
        (3, "ProductName", "product_name"),
        # (3, "ProductName", "product_name"),
        (3, "Price", "price"),

    ]

    df = spark.createDataFrame(data_config, schema=schema)
    df.show()
    return df


df = config_table_two()
uploadIntoMySql(df, "config_table_two", "silver")
# print("uploading config table one in silver database sucessful")


