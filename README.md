# Rules-Based-Algorithm-Insurance

#### This was the Script to generate our synthetic dataset called 'generator_for_parser_script.py'


```
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, randn, round, when, monotonically_increasing_id

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Generate Healthcare Claims Data") \
    .getOrCreate()

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

```


#### The Synthethic dataset that was generated from the script was called 'input_data.csv'. and a snippet of it looks this way.


    id	claim_amount	provider_type	patient_age	claim_type	procedure_complexity	diagnosis_code	treatment_cost	service_duration	out_of_network
    60129542144	3038.66	Clinic	51	Outpatient	Medium	B002	493.09	9	Yes
    60129542145	8541.1	Hospital	37	Inpatient	Low	A001	1056.93	8	No
    60129542146	1023.3	Clinic	26	Inpatient	Medium	A001	4906.56	7	Yes
    60129542147	3158.64	Pharmacy	49	Inpatient	Medium	A001	1697.33	0	Yes
    60129542148	5225.07	Hospital	26	Inpatient	High	B002	105.87	3	No
    60129542149	321.59	Pharmacy	50	Outpatient	Medium	A001	2882.02	1	No
    60129542150	4011.14	Clinic	38	Outpatient	High	B002	1244.52	9	Yes
    60129542151	320.87	Clinic	22	Inpatient	High	A001	2069.36	9	Yes
    60129542152	6762.17	Pharmacy	42	Inpatient	Medium	B002	830.69	2	No
    60129542153	5017.23	Hospital	33	Outpatient	High	A001	4539.21	0	Yes
    60129542154	9296.99	Clinic	23	Inpatient	Low	A001	4070.18	4	Yes




#### SCRIPT FOR APPLYING RULES-BASED ALGORITHM (also uploaded to the repo section as python file)

```
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

# Write labeled data to CSV
final_data.write.csv(output_file, header=True, mode='overwrite')

print(f"Labeled data saved to {output_file}")

# Stop Spark session
spark.stop()

```


#### Rules-based script produced this data with labels "Legitimate", "Suspicious" and "Fraudulent".

```
id	claim_amount	provider_type	patient_age	claim_type	procedure_complexity	diagnosis_code	treatment_cost	service_duration	out_of_network	Final_Label
60129542144	3038.66	Clinic	51	Outpatient	Medium	B002	493.09	9	Yes	Suspicious
60129542145	8541.1	Hospital	37	Inpatient	Low	A001	1056.93	8	No	Fraudulent
60129542146	1023.3	Clinic	26	Inpatient	Medium	A001	4906.56	7	Yes	Suspicious
60129542147	3158.64	Pharmacy	49	Inpatient	Medium	A001	1697.33	0	Yes	Suspicious
60129542148	5225.07	Hospital	26	Inpatient	High	B002	105.87	3	No	Fraudulent
60129542149	321.59	Pharmacy	50	Outpatient	Medium	A001	2882.02	1	No	Suspicious
60129542150	4011.14	Clinic	38	Outpatient	High	B002	1244.52	9	Yes	Suspicious
60129542151	320.87	Clinic	22	Inpatient	High	A001	2069.36	9	Yes	Suspicious
60129542152	6762.17	Pharmacy	42	Inpatient	Medium	B002	830.69	2	No	Fraudulent
60129542153	5017.23	Hospital	33	Outpatient	High	A001	4539.21	0	Yes	Fraudulent
60129542154	9296.99	Clinic	23	Inpatient	Low	A001	4070.18	4	Yes	Fraudulent
60129542155	3343.47	Pharmacy	18	Inpatient	Medium	A001	1342.86	1	Yes	Suspicious
60129542156	9485.43	Hospital	34	Inpatient	High	A001	1948.74	2	Yes	Fraudulent
```
