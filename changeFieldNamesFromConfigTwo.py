from readCSVFile import read_one_col_from_sql_table
from uploadIntoMYSql import uploadIntoMySql
from config_table_one import config_table_one
from config_table_two import config_table_two

config_table_one = config_table_one()
config_table_two = config_table_two()
def changeFieldNamesFromConfigTwo(table_one, table_two):
    for row in table_one.collect():
        table_one_id = row['id']  # or row[0]
        # print(f"{table_one_id} from table one")
        table_one_source_database = row['sourcedatabase']
        table_one_source_table = row['sourcetable']
        print(table_one_source_database)
        print(table_one_source_table)

        fields_to_extract = (table_two.filter(table_two.config_table_id == table_one_id) \
                             .select("sourcefield", "destinationfield").collect())
        print(fields_to_extract)

        source_fields = [field['sourcefield'] for field in fields_to_extract]
        dest_fields = [field['destinationfield'] for field in fields_to_extract]
        print(source_fields)
        print(dest_fields)

        try:
            data = read_one_col_from_sql_table(source_fields, table_one_source_database, table_one_source_table)

            data.show()
        except Exception as e:
            print(f"Error loading table {table_one_source_table} from database {table_one_source_database}: {e}")
            continue
        #  zip function is used to pair each element from the source_fields with the destination fields
        for src, dest in zip(source_fields, dest_fields):
            data = data.withColumnRenamed(src, dest)
        table_name = f"v2{table_one_source_table}"
        uploadIntoMySql(data, table_name, 'gold')


changeFieldNamesFromConfigTwo(config_table_one,config_table_two)

