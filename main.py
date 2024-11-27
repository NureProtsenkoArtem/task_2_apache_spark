import time
from pyspark.sql import SparkSession
import random
import csv

def generate_data(file_name, rows):
    with open(file_name, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["ID", "Value"])
        for i in range(rows):
            writer.writerow([i, random.randint(1, 100)])
            
def process_with_spark(file_name):
    spark = SparkSession.builder.appName("SparkExample").getOrCreate()

    start_time = time.time()
    data = spark.read.csv(file_name, header=True, inferSchema=True)
    
    avg_value = data.groupBy().avg("Value").collect()[0][0]
    end_time = time.time()
    
    spark.stop()
    return avg_value, end_time - start_time

def process_with_standard_tools(file_name):
    start_time = time.time()
    
    with open(file_name, "r") as file:
        reader = csv.DictReader(file)
        values = [int(row["Value"]) for row in reader]
    
    avg_value = sum(values) / len(values)
    end_time = time.time()
    
    return avg_value, end_time - start_time

if __name__ == "__main__":
    file_name = "large_dataset.csv"
    rows = 10_000_000

    generate_data(file_name, rows)

    avg_spark, time_spark = process_with_spark(file_name)
    print(f"Avg variable (Spark): {avg_spark}, Time execution: {time_spark:.2f} seconds")

    avg_python, time_python = process_with_standard_tools(file_name)
    print(f"Avg variable  (Python): {avg_python}, Time execution: {time_python:.2f} seconds")

    speedup = time_python / time_spark
    print(f"\nSpark faster than standart libraries {speedup:.2f} times")