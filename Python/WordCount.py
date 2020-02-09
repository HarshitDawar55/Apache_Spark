from pyspark import SparkContext, SparkConf

# Initializing Spark Configuration
conf = SparkConf().setAppName("WordCount")

import logging

# Initializing Logger
s_logger = logging.getLogger("py4j.java_gateway")
s_logger.setLevel(logging.ERROR)

# Initializing Spark Context
sc = SparkContext(conf = conf)

# Attaching a text file to operate on
text_file = sc.textFile("<Input file path>")

# Main Mapping and Reducing 
counts = text_file.flatMap(lambda line: line.split(" ")).map(word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Loop to print reduced code
for word in counts.collect():
	print(word)

# Stopping SparkContext
sc.stop()