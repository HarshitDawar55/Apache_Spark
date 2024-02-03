from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()
logs = spark.read.text("Logs.txt")
logs.createOrReplaceTempView("Logs")
result = spark.sql("SELECT * FROM Logs where value like '%INFO%'")
result.show(truncate=False)
