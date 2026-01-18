from pyspark.sql import SparkSession

def countLargeDownloads(sc, rdd):
    accum = sc.accumulator(0)
    rdd.foreach(lambda x: accum.add(1) if int(x[2].strip('"')) >= 1000000 else None)
    return accum.value

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("TestApp").getOrCreate()
    sc = spark.sparkContext

    logFileLoc = "D:\\pyspark-training\\data\\log_file.txt"
    logFile = sc.textFile(logFileLoc)
    header = logFile.first()
    logFileData = logFile.filter(lambda x: x != header)
    logFileDataRDD = logFileData.map(lambda x: x.split(","))
    #print("Total Records: ", logFileData.take(2))
    #print(logFileData.take(5))    
    #Assignment 1: Total number of downloads by country and r_os, sorted in descending order
    totalNumberofDownloadsRDD = logFileDataRDD.map(lambda x: ((x[8], x[5]), 1))
    counts = totalNumberofDownloadsRDD.reduceByKey(lambda a, b: a + b)
    totalNumberOfDownloadsSorted = counts.sortBy(lambda x: x[1], ascending=False)
    print("Total Downloads by Country and r_os (sorted descending): ", totalNumberOfDownloadsSorted.take(10))

    #Assignment 2: Use Accumulator to count number of large downloads (size >= 1,000,000)
    totalCount = countLargeDownloads(sc, logFileDataRDD)
    print(f"Total number of records where size >= 1000000: {totalCount}")

    spark.stop()