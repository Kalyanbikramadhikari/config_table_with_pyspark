
from readCSVFile import readCSVFile
from pyspark.sql.functions import col, to_date,date_format
from uploadIntoMYSql import uploadIntoMySql

fc_account_master, fc_transaction_base, salesTransaction = readCSVFile()
def adjust_date_from_raw(table,table_name_to_keep_in_sql_table, date_column):
    if(date_column == "Date"):
        table = table.withColumn("Date", date_format(
            to_date(table["Date"], "MM/dd/yyyy"), "yyyy-MM-dd"))
        table = table.withColumn("Date", to_date(col("Date"), 'yyyy-MM-dd'))

        # store into parquet file
        table.write.parquet(f"D:/internship-f1soft/silver/{table_name_to_keep_in_sql_table}", mode='overwrite')
        print(f"{table_name_to_keep_in_sql_table} successfully stored in parquet file")

        # upload the table
        uploadIntoMySql(table, table_name_to_keep_in_sql_table, 'silver' )

        # print(f"{table_name_to_keep_in_sql_table} successfully uploaded into silver database")
    else:
        table = table.withColumn(date_column, date_format(date_column, "yyyy-MM-dd"))
        table = table.withColumn(date_column, to_date(col(date_column), "yyyy-MM-dd"))

        # store into parquet file
        table.write.parquet(f"D:/internship-f1soft/silver/{table_name_to_keep_in_sql_table}", mode='overwrite')
        print(f"{table_name_to_keep_in_sql_table} successfully stored in parquet file")
        #  upload into mysql

        uploadIntoMySql(table, table_name_to_keep_in_sql_table, 'silver')
        # print(f"{table_name_to_keep_in_sql_table} successfully uploaded into silver database")

# # with "" means that will the name of the table in gold database and without "" means the table of raw
fc_account_master = adjust_date_from_raw(fc_account_master, "fc_account_master",'acc_open_date' )
fc_transaction_base = adjust_date_from_raw( fc_transaction_base,"fc_transaction_base",'ï»¿tran_date')
# # fc_transaction_base.show()

salesTransaction = adjust_date_from_raw(salesTransaction,"salesTransaction", 'Date')
# salesTransaction.show()

