// Set the file path which contains text
val myfile = sc.textFile("<FilePath>")

// Apply FlatMap, Map and ReduceByKey functions for wordcount
val counts = myfile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)

// Apply collect function to see the final output
counts.collect()