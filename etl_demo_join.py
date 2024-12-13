import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


# Initialize Spark session
spark = SparkSession.builder.appName("SimpleETL").getOrCreate()

#Extract
# Load data from CSV files
print("Extracting data from source ...")
customer_table = pd.read_csv('/data/input/customer_table.csv')
product_table = pd.read_csv('/data/input/product_table.csv')
order_table = pd.read_csv('/data/input/order_table.csv')

#Transform
# Merge tables step by step
print("Transforming the data ...")
# Step 1: Merge order_table with customer_table on customer_id
customer_order = pd.merge(order_table, customer_table, on='customer_id', how='inner')

# Step 2: Merge the result with product_table on product_id
customer_order = pd.merge(customer_order, product_table, on='product_id', how='inner')

# Select the required columns for the final table
customer_order = customer_order[[
    'customer_id', 'order_id', 'customer_name', 'customer_address', 'product_id', 
    'product_name', 'order_date', 'order_qty', 'total_price'
]]

#Load
# Save the merged table as a CSV file
customer_order.to_csv('/data/output/customer_order.csv', index=False)
print("Data loaded to output table ...")

spark.stop()
