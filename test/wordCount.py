from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("TestApp").getOrCreate()
    sc = spark.sparkContext

    ebookFileLoc = "file:/D:/pyspark-training/data/pg1342.txt"
    ebookRDD = sc.textFile(ebookFileLoc)
    print(ebookRDD.take(5)) # Print first 5 lines of the ebook
    ebookRdd2 = ebookRDD.filter(lambda x:x.strip() != "") # Remove empty lines
    spark.stop()