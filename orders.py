import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


def clean_data(df):
    """
    Keep only orders with a value greater than zero.
    Cast value to double in case the CSV reader loads it as text.
    """
    return (
        df.withColumn("amount", col("amount").cast("double"))
        .withColumn("order_Date", to_date(col("order_Date"), "yyyy-MM-dd"))
        .withColumn("amount_with_tax", col("amount")*1.20)
        .filter(col("product").isNotNull())
        .filter(col("amount") > 0)
    )
    pass


def main(csv_path):
    spark = (
        SparkSession.builder
        .appName("CustomerOrderCleaning")
        .getOrCreate()
    )

    try:
        # Read the CSV file.
        # Example path:
        # hdfs:///data/customer_orders.csv
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("mode", "PERMISSIVE")
            .csv(csv_path)
        )

        # Apply cleaning transformations.
        cleaned = clean_data(df)

        # Show the cleaned orders in the Spark output/logs.
        cleaned.show(truncate=False)

        # Optional: write the cleaned data as CSV.
        # Note: Spark writes a folder containing one or more CSV part files.
        #
        # (
        #     cleaned.write
        #     .mode("overwrite")
        #     .option("header", "true")
        #     .csv("hdfs:///data/cleaned_customer_orders")
        # )

        return cleaned

    finally:
        spark.stop()

if __name__ == "__main__":
    import sys
    main(sys.argv[1])
