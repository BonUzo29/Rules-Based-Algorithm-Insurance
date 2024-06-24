from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, when, col
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Healthcare Claims Labeling") \
    .getOrCreate()

# Set log level to OFF
spark.sparkContext.setLogLevel("OFF")

# Define input and output file paths
input_file = 'input_data.csv'
output_file = 'labeled_data.csv'

# Load input data
data = spark.read.csv(input_file, header=True, inferSchema=True)

# Define rules as functions
def rule_is_high_claim_amount(claim_amount):
    return 'Fraudulent' if claim_amount > 5000 else 'Legitimate'

def rule_is_hospital_inpatient(provider_type, claim_type):
    if provider_type == 'Hospital' and claim_type == 'Inpatient':
        return 'Legitimate'
    else:
        return 'Suspicious'

# Create UDFs (User Defined Functions) for rules
udf_high_claim_amount = udf(rule_is_high_claim_amount, StringType())
udf_hospital_inpatient = udf(rule_is_hospital_inpatient, StringType())

# Apply rules and create new columns for labels
data_labeled = data.withColumn('Label1', udf_high_claim_amount('claim_amount'))
data_labeled = data_labeled.withColumn('Label2', udf_hospital_inpatient('provider_type', 'claim_type'))

# Combine labels based on multiple rules if needed
data_labeled = data_labeled.withColumn('Final_Label', when((col('Label1') == 'Fraudulent') | (col('Label2') == 'Fraudulent'), 'Fraudulent')
                                     .when((col('Label2') == 'Legitimate'), 'Legitimate')
                                     .otherwise('Suspicious'))

# Select columns of interest for final output
final_data = data_labeled.select('id', 'claim_amount', 'provider_type', 'patient_age', 'claim_type', 
                                 'procedure_complexity', 'diagnosis_code', 'treatment_cost', 
                                 'service_duration', 'out_of_network', 'Final_Label')

# Coalesce to a single partition to write a single CSV file
final_data = final_data.coalesce(1)

# Write labeled data to CSV
final_data.write.csv(output_file, header=True, mode='overwrite')

print(f"Labeled data saved to {output_file}")

# Stop Spark session
spark.stop()
