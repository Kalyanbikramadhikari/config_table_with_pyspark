from buildSparkSession import initialize_spark_session
spark,*_, connection_properties = initialize_spark_session()

def readCSVFile():
    fc_account_master = spark.read.csv("D:/internship-f1soft/day-3/assignment_2/fc_account_master.csv", header=True,
                              inferSchema=True)
    fc_transaction_base = spark.read.csv("D:/internship-f1soft/day-3/assignment_2/fc_transaction_base1.csv", header=True,
                          inferSchema=True)
    salesTransaction = spark.read.csv("D:/internship-f1soft/datasets/Sales Transaction v.4a.csv", header=True,
                                inferSchema=True)
    print('Read Sucessfull')

    return fc_account_master, fc_transaction_base, salesTransaction

def read_from_sql_table_betweendates(database_name, table_name,date_column, start_date, end_date, incremental_flag):
    jdbcUrl = f"jdbc:mysql://localhost:3306/{database_name}"
    if incremental_flag == 1:
        query = f"(SELECT * FROM {table_name} WHERE {date_column} BETWEEN '{start_date}' AND '{end_date}'ORDER BY ({date_column}) ASC) AS temp"
        # query = f"(SELECT * FROM {table_name} WHERE {date_column} BETWEEN '{start_date}' AND '{end_date}' ORDER BY {date_column}) AS temp"


    elif incremental_flag == 0:
        query = f"(SELECT * FROM {table_name} WHERE {date_column} ='{end_date}' ) AS temp"

    data_from_sql = spark.read.jdbc(url=jdbcUrl, table=query, properties=connection_properties)
    return data_from_sql


def read_one_col_from_sql_table(source_fields, database_name,table_one_source_table ):
    jdbcUrl = f"jdbc:mysql://localhost:3306/{database_name}"
    query = f"(SELECT {', '.join(source_fields)} FROM {table_one_source_table}) AS subquery"

    # query = f"SELECT {', '.join(source_fields)} FROM {table_one_source_table} AS subquery"


    data_from_sql = spark.read.jdbc(url=jdbcUrl,table=query, properties=connection_properties)
    return data_from_sql

