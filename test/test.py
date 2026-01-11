from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("TestApp").getOrCreate()
    sc = spark.sparkContext
    list1 = [1, 2, 3, 4, 5]
    rdd1 = sc.parallelize(list1)
    result = rdd1.map(lambda x: x * 10)
    print(result.collect())
    spark.stop()