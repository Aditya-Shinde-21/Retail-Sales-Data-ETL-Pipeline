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

    return result_df
