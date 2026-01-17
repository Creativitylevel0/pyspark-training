from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def getDfFromTextFileWithHeader(sparksession, filePath, fileSchema, filedelim=","):
    outputDF = (sparksession.read
                .option("header", "true")
                .option("sep", filedelim)
                .schema(fileSchema)
                .csv(filePath)
                )
    return outputDF
#Assignment 3
def transformDf(df):
    transformedDF = (df.select("*")
                    .where(df.package != "NA")
                    .orderBy(df.date, df.time))
    return transformedDF
#Assignment 4
def addColumnToTransformedDf(df):
    newColumnDF = df.withColumn("download_type",when(df.size<=1000000,"small").otherwise("large"))
    return newColumnDF
#Assignment 5
def downloadCountByPackage(df):
    packageDf = (df.groupby("package").agg(sum("size").alias("Package_Count")))
    return packageDf
#Assignment 6
def aggregateByDownloadTypeAndDate(df):
    aggregatedDF = (
        df.groupBy("download_type", "date")
          .agg(
              sum("size").alias("Total_size"),
              round(avg("size"), 0).alias("average_size")
          )
    )
    return aggregatedDF

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("pySpark Application").getOrCreate()
    logFileLoc = "D:\\pyspark-training\\data\\log_file.txt"
    logFileSchema = ("date DATE,"
                     "time TIMESTAMP,"
                     "size INT,"
                     "r_version FLOAT,"
                     "r_arch STRING,"
                     "r_os STRING,package STRING,"
                     "version STRING,"
                     "country STRING,"
                     "ip_id INT")

    logFileDF = getDfFromTextFileWithHeader(spark, logFileLoc, logFileSchema)

    logFileTransformed = transformDf(logFileDF)
    logFileTransformedAddColumn = addColumnToTransformedDf(logFileTransformed)
    packageCount = downloadCountByPackage(logFileTransformed)
    aggregateDf = aggregateByDownloadTypeAndDate(logFileTransformedAddColumn)

    logFileDF.show(5)
    #logFileTransformedAddColumn.printSchema()
    logFileTransformedAddColumn.show(5)
    packageCount.show(5)
    aggregateDf.show(20)

    spark.stop()