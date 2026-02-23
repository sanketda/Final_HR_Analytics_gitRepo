from pyspark.sql import SparkSession

# Simple PySpark job to load and print a CSV file
def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("CSVLoadJob") \
        .master("local[*]") \
        .getOrCreate()

    # Read CSV file with header and schema inference
    df = spark.read.csv(
        "orders_100_balanced.csv", 
        header=True, 

        inferSchema=True
    )

    # Show the first 20 rows
    df.show()

    # Print the schema of the DataFrame
    df.printSchema()

    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
