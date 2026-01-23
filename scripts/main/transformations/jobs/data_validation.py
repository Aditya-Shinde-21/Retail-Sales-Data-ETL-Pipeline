from pyspark.sql.functions import col, when

from scripts.main.utility.logging_config import logger
from scripts.main.write.dataframe_format_writer import FormatWriter


class DataQualityValidator:
    """
    Validates sales data, quarantines invalid records to S3,
    and returns clean records for downstream processing.
    """
    def __init__(self, s3_invalid_path):
        self.s3_invalid_path = s3_invalid_path

    def validate_and_split(self, sales_df):
        """
        Flags invalid records, writes them to S3, and returns clean DataFrame.
        """
        # Flag invalid records
        df_with_validation_flag = sales_df.withColumn(
            "validation_error",
            when(col("customer_id").isNull(), "customer_id_missing")
            .when(col("product_id").isNull(), "product_id_missing")
            .when(col("sales_person_id").isNull(), "sales_person_id_missing")
            .when(col("sales_date").isNull(), "sales_date_missing")
            .when(col("quantity") <= 0, "invalid_quantity")
            .when(col("price") <= 0, "invalid_price")
            .otherwise(None)
        )

        invalid_df = df_with_validation_flag.filter(col("validation_error").isNotNull())
        invalid_count = invalid_df.count()
        logger.info("Invalid record count: %s", invalid_count)

        if invalid_count > 0:
            try:
                parquet_writer = FormatWriter(mode="append", data_format="parquet")
                parquet_writer.write_to_format(invalid_df, self.s3_invalid_path)
                logger.info("Invalid records written to '%s'", self.s3_invalid_path)

            except Exception:
                logger.exception("Invalid data write to s3 failed")
                raise

        # ------------------------------------------------------------------
        # Persist clean data for downstream reuse
        clean_df = df_with_validation_flag.filter(col("validation_error").isNull())
        valid_count = clean_df.count()
        logger.info("Valid record count: %s", valid_count)

        if valid_count == 0:
            logger.error("'clean_df' is empty. Pipeline cannot proceed")
            raise RuntimeError("No clean records to process")

        return clean_df