from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *



if __name__ == "__main__":

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("DataFrame Streaming Application")
        .getOrCreate()
    )

        
    orderDetailStreamLoc  = "D:/pyspark-training/data/retailstream/data"
    orderdetailOutputLoc  = "D:/pyspark-training/data/retailstream/output"
    orderdetailCheckLoc   = "D:/pyspark-training/data/retailstream/check"

    orderDetailSchema = "orderNumber int, productCode string, quantityOrdered int,priceEach decimal(10,2),orderLineNumber int"

    orderDetailStreamDf = (spark
                           .readStream
                           .format("csv")
                           .schema(orderDetailSchema)
                           .option("header","true")
                           .option("maxFilesPerTrigger","1")
                           .load(orderDetailStreamLoc)
    )

    orderDetailStreamDf1 = orderDetailStreamDf \
                        .withColumn("input_file_name", input_file_name())\
                        .withColumn("input_time_stamp", current_timestamp())
    
    odStream = (orderDetailStreamDf1
                .writeStream \
                .format("json") \
                .option("checkpointLocation", orderdetailCheckLoc) \
                .trigger(processingTime="5 seconds") \
                .outputMode("append") \
                .start(orderdetailOutputLoc)
    )

    odStream.awaitTermination()


