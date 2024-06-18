from readCSVFile import read_from_sql_table_betweendates
from buildSparkSession import initialize_spark_session
from config_table_one import config_table_one
import pymysql

connection_pymysql = pymysql.connect(
    host='localhost',
    user='root',
    password='1234',
    database='config_datbase'
)

_, _, _, _, _, connection_properties = initialize_spark_session()
df = config_table_one()



def transfer_data_between_dates(row):
    source_db = row['sourcedatabase']
    source_table = row['sourcetable']
    destination_db = row['destinationdatabase']
    destination_table = row['destinationtable']
    incremental_flag = row['incrementalflag']
    date_col = row['sourcetable_datecol']  # Replace with the actual date column name in your source tables
    startdate = row['startdate']
    enddate = row['enddate']
    print(source_db)



    source_data = read_from_sql_table_betweendates(source_db, source_table ,date_col, startdate, enddate, incremental_flag)
    source_data.show()    # print('hello')
    if incremental_flag == 1:
        write_mode = "append"
    elif incremental_flag == 0:
        write_mode = "overwrite"
    uploadUrl =f"jdbc:mysql://localhost:3306/{destination_db}"
    #
    # #
    source_data.write.jdbc(url=uploadUrl, table=f"{destination_table}", mode=write_mode,
                           properties=connection_properties)
    print(f'{source_table} sucessfully uploaded into {destination_db}')
    print('hey bro')
    #


for row in df.collect():
    transfer_data_between_dates(row)
    with connection_pymysql.cursor() as cursor:
        # startdate_query = "update config_datbase.config_table set startdate = date_add(startdate, interval '1 day')"
        # cursor.execute(startdate_query)

        enddate_query = "update silver.config_table_one set enddate = date_add(enddate, interval 1 day)"
        cursor.execute(enddate_query)

        connection_pymysql.commit()
        print("config table one sucessfully updated")