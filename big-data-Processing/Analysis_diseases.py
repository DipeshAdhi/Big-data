from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, round

# Initialize Spark Session
spark = SparkSession.builder.appName("HighRiskPatientAnalysis").getOrCreate()

# Load the Cleaned Data
df = spark.read.csv("hdfs://namenode:9000/user/data/Cleaned_Patient.csv", header=True, inferSchema=True)

# Step 1: Calculate BMI (weight in kg, height in meters)
df = df.withColumn("BMI", round(col("weight") / (col("height") / 100) ** 2, 2))

# Step 2: Define Risk Criteria
# High BMI (>= 30), Irregular blood pressure (e.g., bloos_pressure >= 140), High sugar levels (>= 200), Smoking history
df = df.withColumn(
    "HighRisk",
    when((col("BMI") >= 30) | 
         (col("bloos_pressure") >= 140) | 
         (col("sugar_level") >= 200) | 
         (col("smoking_history") == "true"), lit("Yes")).otherwise("No")
)

# Step 3: Filter High-Risk Patients
high_risk_patients = df.filter(col("HighRisk") == "Yes")

# Step 4: Count High-Risk Patients
high_risk_count = high_risk_patients.count()
print(f"Number of High-Risk Patients: {high_risk_count}")



# Display High-Risk Patients
high_risk_patients.show(truncate=False)

# Step 4: Save High-Risk Patients to HDFS
high_risk_patients.write.csv("hdfs://namenode:9000/user/data/HighRisk_Patients.csv", header=True, mode="overwrite")