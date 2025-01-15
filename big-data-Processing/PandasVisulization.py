import pandase as pd
import matplotlib.pyplot as plt

# Load the dataset
df = pd.read_csv('Patient.csv')

# Show the first few rows of the data
print(df.head())

# Step 1: Count of High-Risk Patients by Risk Factor
# Apply business logic for BMI, blood pressure, and smoking history
df['Risk_Factor'] = df.apply(lambda row: [
    'BMI' if row['BMI'] >= 30 else None,
    'bloos_pressure' if row['bloos_pressure'] >= 140 else None,
    'smoking_history' if row['smoking_history'] == "true" else None
], axis=1)

# Remove None values and count risk factors
df['Risk_Factor_Count'] = df['Risk_Factor'].apply(lambda x: len([i for i in x if i is not None]))

# Step 2: Count how many patients fall into each risk factor category
risk_factor_counts = df['Risk_Factor_Count'].value_counts()

# Plot Bar Chart for Risk Factors
plt.figure(figsize=(10, 6))
risk_factor_counts.plot(kind='bar', color='skyblue')
plt.title('Count of High-Risk Patients by Number of Risk Factors')
plt.xlabel('Number of Risk Factors')
plt.ylabel('Count of Patients')
plt.xticks(rotation=0)
plt.show()

# Step 3: BMI Distribution of High-Risk Patients
plt.figure(figsize=(10, 6))
plt.hist(df['BMI'], bins=30, color='orange', edgecolor='black')
plt.title('BMI Distribution of High-Risk Patients')
plt.xlabel('BMI')
plt.ylabel('Frequency')
plt.show()

# Step 4: Age Distribution of High-Risk Patients
plt.figure(figsize=(10, 6))
plt.hist(df['age'], bins=30, color='green', edgecolor='black')
plt.title('Age Distribution of High-Risk Patients')
plt.xlabel('Age')
plt.ylabel('Frequency')
plt.show()