from buildSparkSession import initialize_spark_session
from pyspark.sql.types import StructField,DateType, StructType, IntegerType, StringType
from uploadIntoMYSql import uploadIntoMySql
import datetime


spark,*_ = initialize_spark_session()


def config_table_one():
    schema = StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("sourcedatabase", StringType(), nullable=False),
        StructField("sourcetable", StringType(), nullable=False),
        StructField("sourcetable_datecol", StringType(), nullable=False),
        StructField("destinationdatabase", StringType(), nullable=False),
        StructField("destinationtable", StringType(), nullable=False),
        StructField("incrementalflag", IntegerType(), nullable=False),
        StructField("startdate", DateType(), nullable=True),
        StructField("enddate", DateType(), nullable=True)
    ])

    data_config = [
        (1, "silver", "fc_account_master", "acc_open_date", "gold",
         "fc_account_master", 0, datetime.date(1900, 1, 1), datetime.date(2025, 1, 1)),
        (2, "silver", "fc_transaction_base", "ï»¿tran_date", "gold",
         "fc_transaction", 1, datetime.date(1990, 1, 1), datetime.date(2025, 1, 1)),
        (3, "silver", "salestransaction", "Date", "gold",
         "sales_transaction", 1, datetime.date(1990, 1, 1), datetime.date(2025, 1, 1))
    ]

    df = spark.createDataFrame(data_config, schema=schema)
    # df.show()
    # print('hello')
    return df


df = config_table_one()
uploadIntoMySql(df, "config_table_one", "silver")
# print("uploading config table one in silver database sucessful")


