import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


# Initialize Spark session
spark = SparkSession.builder.appName("SimpleETL").getOrCreate()

# Extract: Read data from a CSV file
#input_path = "/data/input/sample_data.csv"  # Adjust the path as needed
input_path = "/data/input/employees_db.csv"
df = pd.read_csv(input_path, delimiter=",")

# Transform: Perform a simple transformation (filter data)

# Transform: Increase salary by 10% for employees in the Engineering department and store as revised salary
df["revised_salary"] = df["salary"].where(
    df["department"] != "Engineering",  # Keep the same salary if not Engineering
    df["salary"] * 1.10                 # Increase by 10% if Engineering
)

# Load: Write the transformed data to a new CSV file
output_path = "/data/output/transformed_data.csv"  # Adjust the path as needed
df.to_csv(output_path, index=False)

spark.stop()
