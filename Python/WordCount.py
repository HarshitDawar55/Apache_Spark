from pyspark import SparkContext, SparkConf

# Initializing Spark Configuration
conf = SparkConf().setAppName("WordCount")

import logging

logger = logging.getLogger("WordCount")
log_file_handler = logging.FileHandler("Logs.txt")
log_file_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s"
    )
)
logger.addHandler(log_file_handler)
# Initializing Logger
logging.basicConfig(
    format="%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s",
    level=logging.INFO,
)
s_logger = logging.getLogger("py4j.java_gateway")
s_logger.setLevel(logging.ERROR)

# Initializing Spark Context
logger.info("Initializing the Spark Context")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# Attaching a text file to operate on
logger.info("Reading a Text File")
text_file = sc.textFile("Demo_Data.txt")

# Main Mapping and Reducing
logger.info("Starting the MapReduce Transformation")
counts = (
    text_file.flatMap(lambda line: line.split(" "))
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
)
logger.info("MapReduce Transformation Finished")

logger.info("Printing the Word Count")
# Loop to print reduced code
for word in counts.collect():
    print(word)

logger.info("Stopping the Spark Context!")
# Stopping SparkContext
sc.stop()
