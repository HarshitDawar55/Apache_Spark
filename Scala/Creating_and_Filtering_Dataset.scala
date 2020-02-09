// Creating a dataset which contains values in range(1, 50000)
// Due to lazy evaluation only a part of this dataset will be created now, complete will be created when it is required!
val data = 1 to 50000;

// Partitioning Data in chunks of 30 values each, Now complete Dataset will be created!
val SparkSample = sc.parallelize(data, 30);

// Filtering the values from the dataset which are less than 10
SparkSample.filter(_ <10).collect(); 