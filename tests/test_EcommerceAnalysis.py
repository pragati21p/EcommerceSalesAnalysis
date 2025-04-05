# Unit Tests for LoadEcommerceData and refineData classes
import unittest
from unittest.mock import MagicMock, patch, PropertyMock
import pytest

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType


from workspace.EcommerceAnalysis import *

class TestEcommerceDataPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("TestEcommerceData").getOrCreate()
        cls.loader = LoadEcommerceData(cls.spark)
        cls.refiner = refineData()

    def test_clean_columns(self):
        df = self.spark.createDataFrame([(1, "John")], ["Customer ID", "Customer Name"])
        cleaned_df = self.loader.clean_columns(df)
        self.assertEqual(cleaned_df.columns, ["Customer_ID", "Customer_Name"])

    def test_convert_columns_to_lowercase(self):
        df = self.spark.createDataFrame([(1, "John")], ["Customer_ID", "Customer_Name"])
        lower_df = self.refiner.convert_columns_to_lowercase(df)
        self.assertEqual(lower_df.columns, ["customer_id", "customer_name"])

    def test_formatName(self):
        df = self.spark.createDataFrame([(1, "John  Doe!!",)], ["Customer_ID","customer_name"])
        formatted_df = self.refiner.formatName(df)
        result = formatted_df.collect()[0]["customer_name"]
        self.assertEqual(result, "John Doe")

    def test_validateEmail(self):
        df = self.spark.createDataFrame([("user@example.com",1), ("invalid-email",2)], ["email","customer_id"])
        validated_df = self.refiner.validateEmail(df)
        results = [row.email for row in validated_df.collect()]
        self.assertEqual(results, ["user@example.com", None])

    def test_formatPhone(self):
        df = self.spark.createDataFrame([("+1 123-456-7890",1), ("001-9876543210",2), ("5551234",3)], ["phone","customer_id"])
        formatted_df = self.refiner.formatPhone(df, "phone")
        formatted_numbers = [row.phone for row in formatted_df.collect()]
        self.assertEqual(formatted_numbers[0], "123-456-7890")
        self.assertEqual(formatted_numbers[1], "987-654-3210")
        self.assertIsNone(formatted_numbers[2])

    def test_dropna_and_dedup(self):
        df = self.spark.createDataFrame([
            ("1", "Alice"),
            (None, "Bob"),
            ("1", "Alice")
        ], ["customer_id", "customer_name"])
        cleaned_df = df.dropna(subset=["customer_id", "customer_name"]).dropDuplicates(["customer_id"])
        self.assertEqual(cleaned_df.count(), 1)
    
    @patch("pyspark.sql.DataFrame.write")
    @patch("pyspark.sql.SparkSession.read", new_callable=PropertyMock)
    def test_create_master_table(self, mock_write, mock_read_prop):
        mock_writer = MagicMock()
        mock_write.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.saveAsTable.return_value = None

        mock_read = MagicMock()
        mock_read.table.return_value = self.spark.createDataFrame([(1, "John")], ["Customer ID", "Customer"])
        mock_read_prop.return_value = mock_read

        # Sample Orders DF
        orders_df = self.spark.createDataFrame([
            ("O1", "C1", "P1", "10/1/2024", 200.456),
            ("O2", "C2", "P2", "15/2/2024", 150.753),
        ], ["order_id", "customer_id", "product_id", "order_date", "profit"])

        # Sample Customers
        customers_df = self.spark.createDataFrame([
            ("C1", "Alice", "USA"),
            ("C2", "Bob", "India"),
        ], ["customer_id", "customer_name", "country"])

        # Sample Products
        products_df = self.spark.createDataFrame([
            ("P1", "Electronics", "Laptops"),
            ("P2", "Furniture", "Chairs"),
        ], ["product_id", "category", "sub_category"])

        # Save refined versions to delta (as if prepared earlier)
        customers_df.write.format("delta").mode("overwrite").saveAsTable("refined_customers")
        products_df.write.format("delta").mode("overwrite").saveAsTable("refined_products")

        # Call the actual function
        with pytest.raises(Exception):
            assert createMasterTable(self.spark, orders_df)


    @patch("pyspark.sql.DataFrame.write")
    @patch("pyspark.sql.SparkSession.read", new_callable=PropertyMock)
    def test_profit_analysis(self, mock_write, mock_read_prop):
        try:
            mock_writer = MagicMock()
            mock_write.format.return_value = mock_writer
            mock_writer.mode.return_value = mock_writer
            mock_writer.saveAsTable.return_value = None

            mock_read = MagicMock()
            mock_read_prop.return_value = mock_read
            
            # Create dummy detailed_orders table
            df = self.spark.createDataFrame([
                ("O1", "1/1/2024", 200.0, "Alice", "USA", "P1", "Electronics", "Laptops", 2024),
                ("O2", "1/2/2024", 150.0, "Bob", "India", "P2", "Furniture", "Chairs", 2024),
                ("O3", "1/3/2024", 100.0, "Alice", "USA", "P1", "Electronics", "Laptops", 2024),
            ], ["order_id", "order_date", "profit", "customer_name", "country", "product_id", "category", "sub_category", "year"]) \
                .withColumn("order_date", to_date("order_date", "d/M/yyyy")) \
                .withColumn("year", F.col("year").cast(IntegerType()))
            
            df.write.format("delta").mode("overwrite").saveAsTable("detailed_orders")

            # Run aggregation logic
            df = profitAnalysis(self.spark)

            # Validate aggregation
            electronics_alice_profit = df.filter(
                (F.col("category") == "Electronics") &
                (F.col("customer_name") == "Alice")
            ).select("total_profit").collect()[0]["total_profit"]
        except Exception as e:
            self.fail(f"profitAnalysis() raised an exception: {e}")


