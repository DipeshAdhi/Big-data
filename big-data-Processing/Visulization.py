from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns

# Initialize Spark Session
spark = SparkSession.builder.appName("HighRiskVisualization").getOrCreate()

# Load the High-Risk Patients Data
high_risk_df = spark.read.csv("hdfs://namenode:9000/user/data/HighRisk_Patients.csv", header=True, inferSchema=True)

# Step 1: Count of High-Risk Patients by Risk Factor
# Add a new column 'Risk_Factor' which checks risk factors (BMI, blood pressure, sugar level, smoking history)
from pyspark.sql.functions import when, col

high_risk_df = high_risk_df.withColumn(
    "Risk_Factor",
    when(col("BMI") >= 30, "BMI")
    .when(col("bloos_pressure") >= 140, "blood_pressure")
    .when(col("sugar_level") >= 126, "sugar_level")
    .when(col("smoking_history") == "true", "smoking_history")
    .otherwise(None)
)

# Count patients based on risk factors
risk_factor_counts_df = high_risk_df.groupBy("Risk_Factor").count().toPandas()

# Bar Plot for Risk Factors
plt.figure(figsize=(10, 6))
sns.barplot(x="Risk_Factor", y="count", data=risk_factor_counts_df, palette="Blues_d")
plt.title("Count of High-Risk Patients by Risk Factor")
plt.xlabel("Risk Factor")
plt.ylabel("Count of Patients")
plt.xticks(rotation=45)
plt.show()

# Step 2: BMI Distribution of High-Risk Patients
# Filter for patients with BMI >= 30 (assuming high-risk patients have BMI >= 30)
high_bmi_df = high_risk_df.filter(col("BMI") >= 30).toPandas()

plt.figure(figsize=(10, 6))
sns.histplot(high_bmi_df["BMI"], bins=30, kde=True, color="orange")
plt.title("BMI Distribution of High-Risk Patients")
plt.xlabel("BMI")
plt.ylabel("Frequency")
plt.show()

# Step 3: Age Distribution of High-Risk Patients
# Get age distribution for high-risk patients
high_risk_age_df = high_risk_df.filter(col("Risk_Factor").isNotNull()).toPandas()

plt.figure(figsize=(10, 6))
sns.histplot(high_risk_age_df["age"], bins=30, kde=True, color="green")
plt.title("Age Distribution of High-Risk Patients")
plt.xlabel("Age")
plt.ylabel("Frequency")
plt.show()