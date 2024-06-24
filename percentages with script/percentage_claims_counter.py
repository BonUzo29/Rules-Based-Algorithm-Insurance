from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Analyze and Visualize Healthcare Claims Data") \
    .getOrCreate()

# Set log level to OFF
spark.sparkContext.setLogLevel("OFF")

# Define input file path
input_file = 'labeled_data.csv'

# Load labeled data
data = spark.read.csv(input_file, header=True, inferSchema=True)

# Count the occurrences of each label
label_counts = data.groupBy('Final_Label').count().collect()

# Convert to a dictionary for easier processing
label_counts_dict = {row['Final_Label']: row['count'] for row in label_counts}

# Calculate total number of rows
total_rows = sum(label_counts_dict.values())

# Calculate percentages
label_percentages = {label: (count / total_rows) * 100 for label, count in label_counts_dict.items()}

# Print the counts and percentages
print("Counts and Percentages of Labels:")
for label, count in label_counts_dict.items():
    percentage = label_percentages[label]
    print(f"{label}: {count} ({percentage:.2f}%)")

# Plot a pie chart
labels = label_percentages.keys()
sizes = label_percentages.values()

plt.figure(figsize=(10, 6))
plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
plt.title('Distribution of Healthcare Claims Labels')
plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

# Save the pie chart
plt.savefig('label_distribution_pie_chart.png')

# Show the pie chart
plt.show()

# Stop Spark session
spark.stop()
