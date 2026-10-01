import pytest
from pyspark.sql import SparkSession
from orders_file import clean_data
@pytest.fixture(scope="module")
def spark_local():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-customer-orders")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield spark

    spark.stop()


def test_clean_data_invalid_amount(spark_local):
    data = [
        ("1001", "2026-09-01", "laptop", "850.00"),
        ("1002", "2026-09-02", "phone", "-20.00"),
        ("1003", "2026-09-03", "TV", "0"),
        ("1004", "2026-10-1", None, "999.99"),
        ("1005", "2026-10-2", "headphones", "-15.00"),
        ("1006", "2026-10-3", "keyboard", "20"),
    ]

    df = spark_local.createDataFrame(
        data, ["order_id", "order_Date", "product", "amount"]
    )

    transformed_df = clean_data(df)
    results = transformed_df.collect()

    assert len(results) == 1
    assert results[0]["order_id"] == "1001"
    assert results[0]["product"] == "laptop"
    assert results[0]["amount"] == 850.0
    assert results[0]["product"] is not None
    assert results[0]["amount_with_tax"] == (850.0 * 1.20)
    assert len(results) == 1
    assert results[0]["order_id"] == "1006"
    assert results[0]["product"] == "keyboard"
    assert results[0]["amount"] == 20.0
    assert results[0]["product"] is not None
    assert results[0]["amount_with_tax"] == (20.0 * 1.20)