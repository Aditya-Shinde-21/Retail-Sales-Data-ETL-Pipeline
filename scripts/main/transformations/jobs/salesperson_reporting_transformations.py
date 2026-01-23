from pyspark.sql.functions import *
from pyspark.sql.window import Window
from scripts.main.write.database_write import DatabaseWriter

# Calculation for sales team mart
# Find out the total sales done by each sales person every month
# Give the top performer a 1% incentive of total sales of the month
# Write the data into MySQL table

def salesperson_reporting_table_write(final_sales_team_data_mart_df, salesperson_reporting_table):
    # Step 1: Aggregate monthly sales per salesperson
    monthly_sales = final_sales_team_data_mart_df.groupBy(
        "store_id",
        "sales_person_id",
        "sales_person_first_name",
        "sales_person_last_name",
        "sales_date_year",
        "sales_date_month"
    ).agg(sum("total_cost").alias("total_sales"))

    # Step 2: Window for ranking
    rank_window = Window.partitionBy(
        "store_id",
        "sales_date_year",
        "sales_date_month"
    ).orderBy(col("total_sales").desc())

    # Step 3: Rank and incentive calculation
    result_df = monthly_sales.withColumn(
        "dense_rank", dense_rank().over(rank_window)
    ).withColumn("incentive",
        when(col("dense_rank") == 1, round(col("total_sales") * 0.01, 2).alias("total_sales"))
        .otherwise(lit(0))
    ).select(
        "store_id",
        "sales_person_id",
        concat_ws(" ", "sales_person_first_name", "sales_person_last_name").alias("full_name"),
        "sales_date_year",
        "sales_date_month",
        "total_sales",
        "incentive")

    result_df.show()
    # Write the Data into MySQL customers_data_mart table
    db_writer = DatabaseWriter()
    db_writer.write_dataframe(result_df, salesperson_reporting_table)