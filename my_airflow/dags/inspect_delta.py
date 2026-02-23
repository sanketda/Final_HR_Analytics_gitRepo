from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("inspect").getOrCreate()
df = spark.read.format('delta').load('/home/mount_disk/Eclassifier-workspace/Data-engineer-airflow/hr_analytics_dev/delta_lake/hr_analytics/bronze/attendance_raw')

print(f"Total rows in bronze: {df.count()}")
print("Schema:")
df.printSchema()

print("One row of _airbyte_data:")
row = df.select("_airbyte_data").first()
if row and row[0]:
    data = row[0]
    if hasattr(data, 'asDict'):
        print(data.asDict())
    else:
        print(data)

spark.stop()
