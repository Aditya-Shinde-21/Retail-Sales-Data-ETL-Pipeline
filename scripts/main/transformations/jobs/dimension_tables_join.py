from pyspark.sql.functions import *
from scripts.main.utility.logging_config import *
#enriching the data from different table
def dimension_table_joins(sales_df,customer_df,store_df,sales_team_df,product_df):
    # Join fact with dimensions
    joined_df = sales_df.join(customer_df, sales_df.customer_id == customer_df.customer_id, "inner")\
        .join(store_df, sales_df.store_id == store_df.id, "inner")\
        .join(sales_team_df, sales_df.sales_person_id == sales_team_df.id, "inner")\
        .join(product_df, sales_df.product_id == product_df.id, "inner")

    # Select required columns
    result_df = joined_df\
        .select(
            # Customer details
            sales_df.customer_id.alias("customer_id"),
            customer_df.first_name.alias("first_name"),
            customer_df.last_name.alias("last_name"),
            customer_df.address.alias("address"),
            customer_df.pincode.alias("pincode"),
            customer_df.phone_number.alias("phone_number"),

            # Product details
            product_df.id.alias("product_id"),
            product_df.name.alias("product_name"),

            # Sales info
            sales_df.sales_date.alias("sales_date"),
            sales_df.total_cost.alias("total_cost"),
            sales_df.quantity.alias("quantity"),
            sales_df.price.alias("price"),

            # Date breakdown
            year(sales_df.sales_date).alias("sales_date_year"),
            month(sales_df.sales_date).alias("sales_date_month"),

            # Store & salesperson
            sales_df.store_id.alias("store_id"),
            sales_df.sales_person_id.alias("sales_person_id"),

            sales_team_df.first_name.alias("sales_person_first_name"),
            sales_team_df.last_name.alias("sales_person_last_name"),
            store_df.store_manager_name.alias("store_manager_name"),
            sales_team_df.manager_id.alias("manager_id"),
            sales_team_df.address.alias("sales_person_address"),
            sales_team_df.pincode.alias("sales_person_pincode")
        )
    '''
    #step 1 where I am joining customer table
    #But I do not need all the columns so dropping columns which are not needed
    #save the result into s3_customer_df_join
    logger.info("Joining the final_df_to_process with customer_table_df ")
    s3_customer_df_join = final_df_to_process.alias("s3_data") \
        .join(customer_table_df.alias("ct"),
              col("s3_data.customer_id") == col("ct.customer_id"),"inner") \
        .drop("product_id","price","quantity","additional_column",
              "s3_data.customer_id","customer_joining_date")

    final_df_to_process.unpersist()
    #s3_customer_df_join.printSchema()

    #step 2 where I am joining store table details
    #dropping columns which are not needed
    #save the result into s3_customer_store_df_join
    logger.info("Joining the s3_customer_df_join with store_table_df ")
    s3_customer_store_df_join= s3_customer_df_join.join(store_table_df,
                                store_table_df["id"]==s3_customer_df_join["store_id"], "inner")\
                                .drop("id","store_pincode","store_opening_date")

    #step 3 where I am adding sales team table details
    #dropping columns which are not needed
    #save the result into s3_customer_store_sales_df_join
    logger.info("Joining the s3_customer_store_df_join with sales_team_table_df ")
    s3_customer_store_sales_df_join = s3_customer_store_df_join.join(sales_team_table_df.alias("st"),
                             col("st.id")==s3_customer_store_df_join["sales_person_id"], "inner")\
                .withColumn("sales_person_first_name",col("st.first_name"))\
                .withColumn("sales_person_last_name",col("st.last_name"))\
                .withColumn("sales_person_address",col("st.address"))\
                .withColumn("sales_person_pincode",col("st.pincode")) \
                .withColumn("sales_date_year", year(col("sales_date"))) \
                .withColumn("sales_date_month", month(col("sales_date"))) \
                .drop("id","st.first_name","st.last_name","st.address","st.pincode")
    '''
    return result_df