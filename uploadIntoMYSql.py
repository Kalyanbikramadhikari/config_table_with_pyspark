
from buildSparkSession import initialize_spark_session
from readCSVFile import readCSVFile


fc_account_master, fc_transaction_base, salesTransaction = readCSVFile()
spark, mysql_user, mysql_password, mysql_host, mysql_port, connection_properties = initialize_spark_session()

def uploadIntoMySql(dataframe, table_name, database_name):
    jdbcUrl = f"jdbc:mysql://{mysql_host}:{mysql_port}/{database_name}"

    dataframe.write.jdbc(url=jdbcUrl, table=table_name, mode='overwrite', properties=connection_properties)
    print(f"{table_name} sucessfully uploaded into {database_name}")

# uploadIntoMySql(fc_account_master, 'fc_account_master', 'bronze')
# uploadIntoMySql(fc_transaction_base, 'fc_transaction_base', 'bronze')
# uploadIntoMySql(salesTransaction, 'salesTransaction', 'bronze')

