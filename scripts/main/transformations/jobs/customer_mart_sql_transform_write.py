from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
from resources.dev import config
from scripts.main.utility.encrypt_decrypt import decrypt
from scripts.main.write.database_write import DatabaseWriter

#calculation for customer mart
#find out the customer total purchase every month
#write the data into MySQL table

def customer_mart_calculation_table_write(final_customer_data_mart_df):

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
            col("total_sales"))

    '''
    window = Window.partitionBy(col("customer_id"), col("sales_date_year"), col("sales_date_month"))

    final_customer_data_mart = final_customer_data_mart_df\
                    .withColumn("sales_date_year", year(col("sales_date")))\
                    .withColumn("sales_date_month", month(col("sales_date")))\
                    .withColumn("total_sales_every_month_by_each_customer",
                                sum(col("total_cost")).over(window))\
                    .select(col("customer_id"),
                            concat(col("first_name"),lit(" "),col("last_name")).alias("full_name"),
                            col("address"),
                            col("phone_number"),
                            col("sales_date_year"),
                            col("sales_date_month"),
                            col("total_sales_every_month_by_each_customer").alias("total_sales"))\
                    .distinct()

    final_customer_data_mart.show()
    '''
    result_df.persist(StorageLevel.MEMORY_AND_DISK)
    result_df.show()

    #Write the Data into MySQL customers_data_mart table
    db_writer = DatabaseWriter(url=config.url,properties=config.properties)
    db_writer.write_dataframe(result_df,config.customer_data_mart_table)
    result_df.unpersist(blocking=True)