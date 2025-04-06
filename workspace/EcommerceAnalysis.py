# Databricks notebook source
# MAGIC %md
# MAGIC #### Import Packages

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, round, to_date, year, regexp_replace, regexp_extract, when, lit, lower
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Class to Read Files and write Delta Tables

# COMMAND ----------


class LoadEcommerceData():
    def __init__(self, spark):
        self.spark = spark

    def clean_columns(self, df):
        df = df.toDF(*[col.strip().replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "") for col in df.columns])
        return df

    def load_json_data(self, json_path):
        """Loads JSON data into a DataFrame (Orders)."""
        df = self.spark.read.option("multiline", "true").json(json_path)
        return self.clean_columns(df)

    def load_csv_data(self, csv_path):
        """Loads CSV data into a DataFrame (Products)."""
        df = self.spark.read.option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiLine", "true") \
            .option("quote", '"') \
            .option("escape", "\\") \
            .csv(csv_path)
        return self.clean_columns(df)
    
    def load_excel_data(self, excel_path):
        """Loads Excel data into a DataFrame (Customers)."""
        df = self.spark.read.format("com.crealytics.spark.excel") \
            .option("header", True) \
            .option("inferSchema", "true") \
            .option("dataAddress", "WorkSheet") \
            .load(excel_path)
        return self.clean_columns(df)

    def save_as_delta(self, df, table_name):
        """Saves a DataFrame as a Delta table."""
        df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(table_name)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Class to refine data

# COMMAND ----------


class refineData():
    """
    This class is created to clean data
    """
    def convert_columns_to_lowercase(self, df):
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.lower())
        return df

    def formatName(self, df, column_name="customer_name"):
        return df.withColumn( # Step-by-step cleaning pipeline
                column_name,
                regexp_replace(  # Step 3: Remove leading/trailing non-alphabets (e.g. - or spaces)
                    regexp_replace(  # Step 2: Replace multiple spaces with single space
                        regexp_replace(  # Step 1: Keep only letters, hyphens, and spaces
                            col(column_name),
                            r"[^A-Za-z\s-]",
                            ""
                        ),
                        r"\s+",
                        " "
                    ),
                    r"^[^A-Za-z]+|[^A-Za-z]+$",
                    ""
                )
            )

    def validateEmail(self, df, column_name="email"):
        email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        return df.withColumn(
            column_name,
            when(
                regexp_extract(col(column_name), email_regex, 0) != "", 
                col(column_name)
            ).otherwise(lit(None))
        )

    # This logic may differ as per requirement
    def formatPhone(self, df, column_name = "phone"):
        return df.withColumn( # Remove country code from phone number
                column_name,
                regexp_replace(col(column_name), r"^(?:\+1|001)[\s\-\.]?", "")
            ).withColumn( #Remove country code from phone number
                column_name,
                regexp_replace(lower(col(column_name)), "[^0-9x]", "")  # remove all except 0-9 and x
            ).withColumn(
                column_name,
                when(col(column_name).rlike(r"^\d{10}"), col(column_name)).otherwise(None) # starts with 10 digits
            ).withColumn(
                column_name,
                regexp_replace(col(column_name), r"^(\d{3})(\d{3})(\d{4})", r"$1-$2-$3") # place hyphen
            )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create an enriched table which has
# MAGIC - order information 
# MAGIC - Profit rounded to 2 decimal places
# MAGIC - Customer name and country
# MAGIC - Product category and sub category
# MAGIC
# MAGIC **Master Table for further analysis**
# MAGIC

# COMMAND ----------


def createMasterTable(spark, orders_df):
    """
    This function takes orders data and create master delta table
    """
    # Optimize joins by broadcasting small tables
    customers_df = broadcast(spark.read.table("refined_customers"))
    products_df = broadcast(spark.read.table("refined_products"))

    # Join data & compute profit
    orders_df = orders_df.dropna(subset=["order_id"]) \
        .dropDuplicates(["order_id"])

    enriched_orders_df = orders_df \
        .join(customers_df, "customer_id", "left") \
        .join(products_df, "product_id", "left") \
        .select(
            col("order_id"), col("order_date"), col("ship_date"), col("ship_mode"), col("quantity"), col("price"), col("discount"), # Order Detail
            F.round(col("profit"),2).alias("profit"), # Profit
            col("customer_name"), col("country"), # Customer name and country
            col("category"), col("sub_category") # Product category and sub category
        ).withColumn("order_date", to_date(col("order_date"), "d/M/yyyy")).withColumn("year", year(col("order_date")))

    # Partition by year to optimize future queries
    enriched_orders_df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("year").saveAsTable("detailed_orders")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Create an aggregate table that shows profit by 
# MAGIC - Year
# MAGIC - Product Category
# MAGIC - Product Sub Category
# MAGIC - Customer
# MAGIC

# COMMAND ----------

def profitAnalysis(spark):
    """
    This function takes master delta table, analyses profit and save it in another delta table
    """
    detailed_orders_df = spark.read.table("detailed_orders")

    # Add year column and compute aggregates
    agg_df = detailed_orders_df.groupBy("year", "category", "sub_category", "customer_name") \
        .agg(F.sum("profit").alias("total_profit")) \
        .cache()  # Cache to avoid recomputation

    # Partition aggregation table by year
    agg_df.write.format("delta").mode("overwrite") \
        .partitionBy("year").saveAsTable("aggr_profit_analysis")
    return agg_df

# COMMAND ----------

if __name__ == "__main__":
    # Files location
    orders_path = "/FileStore/tables/Orders.json"
    products_path = "/FileStore/tables/Products.csv"
    customer_path = "/FileStore/tables/Customer.xlsx"

    readFilesObj = LoadEcommerceData(spark)

    # 1. Create Raw tables
    # Load Products, Orders, Customers Data
    orders_df = readFilesObj.load_json_data(orders_path)
    products_df = readFilesObj.load_csv_data(products_path).cache()
    customers_df = readFilesObj.load_excel_data(customer_path).cache()

    # Save as Delta tables for faster reads & writes
    readFilesObj.save_as_delta(products_df, "products")
    readFilesObj.save_as_delta(orders_df, "orders")
    readFilesObj.save_as_delta(customers_df, "customers")

    # 2. Create an enriched table for customers and products
    refineObj = refineData()
    enriched_customers_df = refineObj.convert_columns_to_lowercase(customers_df)
    enriched_customers_df = enriched_customers_df.dropna(subset=["customer_id", "customer_name"]) \
                                .dropDuplicates(["customer_id"])

    enriched_customers_df = refineObj.formatName(enriched_customers_df)
    enriched_customers_df = refineObj.validateEmail(enriched_customers_df)
    enriched_customers_df = refineObj.formatPhone(enriched_customers_df)

    readFilesObj.save_as_delta(enriched_customers_df, 'refined_customers')

    enriched_products_df = refineObj.convert_columns_to_lowercase(products_df)
    enriched_products_df = enriched_products_df.dropna(subset=["product_id","category","sub_category"]) \
                                .dropDuplicates(["product_id"])

    readFilesObj.save_as_delta(enriched_products_df, 'refined_products')

    # 3. Create aggregate table/master tale
    createMasterTable(spark, orders_df)

    # 4. Create an aggregate table that shows profit by 4 columns
    profitAnalysis(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using SQL output the following aggregates
# MAGIC - Profit by Year
# MAGIC - Profit by Year + Product Category
# MAGIC - Profit by Customer
# MAGIC - Profit by Customer + Year

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Cache intermediate results for faster access
# MAGIC CACHE TABLE detailed_orders;
# MAGIC
# MAGIC -- Profit by Year (Uses partitioning for fast retrieval)
# MAGIC SELECT year, ROUND(SUM(profit), 2) AS total_profit
# MAGIC FROM detailed_orders
# MAGIC GROUP BY year
# MAGIC ORDER BY year;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Profit by Year + Product Category
# MAGIC SELECT year, category, ROUND(SUM(profit), 2) AS total_profit
# MAGIC FROM detailed_orders
# MAGIC GROUP BY year, category
# MAGIC ORDER BY year, total_profit DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Profit by Customer
# MAGIC SELECT customer_name, ROUND(SUM(profit), 2) AS total_profit
# MAGIC FROM detailed_orders
# MAGIC GROUP BY customer_name
# MAGIC ORDER BY total_profit DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Profit by Customer + Year
# MAGIC SELECT customer_name, year, ROUND(SUM(profit), 2) AS total_profit
# MAGIC FROM detailed_orders
# MAGIC GROUP BY customer_name, year
# MAGIC ORDER BY total_profit DESC;
# MAGIC

# COMMAND ----------

