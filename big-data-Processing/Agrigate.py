from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

# Initialize Spark Session
spark = SparkSession.builder.appName("PatientTreatmentAnalysis").getOrCreate()

# Load data from HDFS
df = spark.read.csv("hdfs://namenode:9000/user/data/Patient_treatment.csv", header=True, inferSchema=True)

# Data Transformation: Group by patient_id and calculate total healthcare costs and treatment count
patient_summary = df.groupBy("patient_id").agg(
    sum("treatment_cost").alias("total_healthcare_cost"),
    count("treatment_id").alias("treatment_count")
)

# Show Results
patient_summary.show()