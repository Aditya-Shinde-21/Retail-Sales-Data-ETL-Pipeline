from pyspark.sql.functions import *
from scripts.main.write.database_write import DatabaseWriter

#calculation for customer mart
#find out the customer total purchase every month
#write the data into MySQL table

def customer_reporting_table_write(final_customer_data_mart_df, customers_reporting_table):

    result_df = final_customer_data_mart_df\
        .groupBy(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("address"),
            col("phone_number"),
            col("sales_date_year"),
            col("sales_date_month"))\
        .agg(sum(col("total_cost")).alias("total_sales"))\
        .select(
            col("customer_id"),
            concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"),
            col("address"),
            col("phone_number"),
            col("sales_date_year"),
            col("sales_date_month"),
            round(col("total_sales"), 2).alias("total_sales"))

    result_df.show()

    #Write the Data into MySQL customers_data_mart table
    db_writer = DatabaseWriter()
    db_writer.write_dataframe(result_df, customers_reporting_table)