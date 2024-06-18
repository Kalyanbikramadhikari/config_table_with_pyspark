
from readCSVFile import readCSVFile
df1, df2, df3 = readCSVFile()


def storeInParquetFile(df1, df2, df3):
    df1.write.parquet("D:/internship-f1soft/bronze/fc_account_master", mode='overwrite')
    df2.write.parquet("D:/internship-f1soft/bronze/fc_transaction_base", mode='overwrite')
    df3.write.parquet("D:/internship-f1soft/bronze/salesTransaction", mode='overwrite')

    print("Raw data sucessfully stored in bronze folder in parquet file")

storeInParquetFile(df1, df2, df3)
