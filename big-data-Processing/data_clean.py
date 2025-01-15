from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, lit
from pyspark.sql.types import DoubleType

# Initialize Spark Session
spark = SparkSession.builder.appName("PatientDataCleaning").getOrCreate()

# Load data from HDFS
df = spark.read.csv("hdfs://namenode:9000/user/data/Patient.csv", header=True, inferSchema=True)

# Step 1: Trim Column Names
df = df.select([col(c).alias(c.strip()) for c in df.columns])

# Step 2: Handle Missing Values
# Replace null patient IDs with a placeholder or drop rows with null patient IDs
df = df.filter(col("patient_id").isNotNull())

# Fill missing numeric values with a default value (e.g., 0 or mean of the column)
numeric_columns = ["bloos_pressure", "sugar_level", "height", "weight"]
for col_name in numeric_columns:
    mean_value = df.select(col_name).dropna().groupBy().avg(col_name).first()[0]
    df = df.withColumn(col_name, when(col(col_name).isNull(), mean_value).otherwise(col(col_name)))

# Fill missing categorical values with a default category (e.g., "unknown")
categorical_columns = ["insurance_details", "smoking_history", "drinking_history"]
df = df.fillna({col_name: "unknown" for col_name in categorical_columns})

# Step 3: Remove Duplicates
df = df.dropDuplicates()

# Step 4: Format Fields
# Standardize gender values
df = df.withColumn("Gender", when(col("Gender").rlike("(?i)^f.*"), lit("Female"))
                   .when(col("Gender").rlike("(?i)^m.*"), lit("Male"))
                   .otherwise(lit("Other")))

# Ensure numeric columns are of appropriate type
for col_name in numeric_columns:
    df = df.withColumn(col_name, col(col_name).cast(DoubleType()))

# Step 5: Validate Columns
# Example: Ensure height and weight are positive
df = df.filter((col("height") > 0) & (col("weight") > 0))

# Show Cleaned Data
df.show()

# Save the cleaned dataset back to HDFS
df.write.csv("hdfs://namenode:9000/user/data/Cleaned_Patient.csv", header=True, mode="overwrite")