from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession.builder \
    .appName("ETL Demo with SQLite") \
    .config("spark.jars", "/opt/spark/jars/sqlite-jdbc-3.47.1.0.jar") \
    .getOrCreate()

# JDBC connection properties
db_url = "jdbc:sqlite:employees.db"
table_name = "employees"
print("Database connected ...")

# Read data from SQLite
df = spark.read.format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", table_name) \
    .option("driver", "org.sqlite.JDBC") \
    .load()

# Transform: Filter Engineering employees and increase their salary by 10%
filtered_df = df.filter(col("department") == "Engineering") \
    .withColumn("salary", col("salary") * 1.10)

# Transform: Select specific columns
result_df = filtered_df.select("id", "name", "salary")

# Write transformed data back to SQLite
output_table_name = "engineering_employees_with_raise"
result_df.write.format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", output_table_name) \
    .option("driver", "org.sqlite.JDBC") \
    .mode("overwrite") \
    .save()

print("Database connected ...")
# Show data in console
result_df.show()

# Stop Spark session
spark.stop()
