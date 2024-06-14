from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, randn, round, when, monotonically_increasing_id

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Generate Healthcare Claims Data") \
    .getOrCreate()
    
# Set log level to OFF
spark.sparkContext.setLogLevel("OFF")

# Number of rows to generate
num_rows = 10000

# Generate random data
data = spark.range(num_rows)

data = data.withColumn('claim_amount', round(rand() * 10000, 2))
data = data.withColumn('provider_type', when(rand() < 0.3, 'Hospital').when(rand() < 0.6, 'Clinic').otherwise('Pharmacy'))
data = data.withColumn('patient_age', (randn() * 10 + 40).cast('int'))
data = data.withColumn('claim_type', when(rand() < 0.5, 'Inpatient').otherwise('Outpatient'))
data = data.withColumn('procedure_complexity', when(rand() < 0.3, 'Low').when(rand() < 0.6, 'Medium').otherwise('High'))
data = data.withColumn('diagnosis_code', when(rand() < 0.5, 'A001').otherwise('B002'))
data = data.withColumn('treatment_cost', round(rand() * 5000, 2))
data = data.withColumn('service_duration', (rand() * 10).cast('int'))
data = data.withColumn('out_of_network', when(rand() < 0.5, 'Yes').otherwise('No'))

# Add unique identifier column
data = data.withColumn("id", monotonically_increasing_id())

# Define output file path
output_file = 'input_data.csv'

# Write data to CSV
data.write.csv(output_file, header=True, mode='overwrite')

print(f"Generated {num_rows} rows of synthetic healthcare claims data using PySpark. Output saved to {output_file}")

# Stop Spark session
spark.stop()
