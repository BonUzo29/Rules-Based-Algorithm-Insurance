# Rules-Based-Algorithm-Insurance

#### Script to generate our synthetic dataset

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


#### The Dataset (input_data.csv)

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
