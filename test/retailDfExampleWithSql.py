from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def getDataframeFromCSVFileWithHeader(sparksession, filePath, fileSchema, filedelim=","):
    outputDF = (sparksession.read
                .option("header", "true")
                .option("sep", filedelim)
                .option("nullValue", "null")
                .schema(fileSchema)
                .csv(filePath)
                )
    return outputDF


if __name__ == '__main__':
    warehouse_location = "hdfs://localhost:9000/user/hive/warehouse"
    spark = (SparkSession
             .builder
             .master("local[*]")
             .appName("pySpark Application")
             .config("spark.sql.warehouse.dir", warehouse_location)
             .enableHiveSupport()
             .getOrCreate())

    orderdetailFileLoc = "/home/azureadmin/training/data/orderdetail.csv"
    ordersFileLoc = "/home/azureadmin/training/data/order.csv"
    ordersSchema = "orderNumber int,orderDate date,requiredDate date,shippedDate date,status string,comments string,customerNumber int"
    productFileLoc = "/home/azureadmin/training/data/product.csv"

    orderdetailDF = (spark.read
                     .format("csv")
                     .option("header", "true")
                     .option("inferSchema", "true")
                     .load(orderdetailFileLoc)
                     )

    # orderdetailDF.printSchema()
    # orderdetailDF.show(5)
    ordersDF = getDataframeFromCSVFileWithHeader(spark, ordersFileLoc, ordersSchema)
    # ordersDF.show(5)

    orderSummaryDF = (ordersDF
                      .join(orderdetailDF, "orderNumber", "inner")
                      .selectExpr("orderNumber", "orderDate", "productCode", "quantityOrdered as qty",
                                  "priceEach as unitPrice")
                      .where(ordersDF.status == "Shipped")
                      )
    orderSummaryDF1 = orderSummaryDF.withColumn("amount", round(col("qty") * col("unitPrice")))
    # orderSummaryDF1.show(5)

    productSummaryDF = (orderSummaryDF1
                        .groupBy("productCode", "orderDate")
                        .agg(sum("qty").alias("salesQty"), sum("amount").alias("salesAmount"))
                        .orderBy(asc("productCode"), desc("orderDate"))
                        )

    # (ordersDF
    # .drop("comments", "requiredDate")
    # .withColumnRenamed("customerNumber", "custNo")
    # .na.fill("Not Avbl", ["status", "comments"])
    # .na.drop(how = "all", subset = ["orderDate", "requiredDate", "shippedDate"])
    # .na.replace("Shipped", "Completed")
    # .where(col("comments").isNotNull())
    # .where(col("comments") != "null")
    # .show())

    # print("Total Records : " + str(totalRecords))
    # productSummaryDF.write.format("json").mode("overwrite").save("/home/azureadmin/output/prodSummary")

    sqlQuery = """select o.orderNumber, o.orderDate, od.productCode, od.quantityOrdered as qty,
                         od.priceEach as unitPrice, o.status
                  from orders o join orderdetails od
                  on (o.orderNumber = od.orderNumber)        
               """

    ordersDF.createOrReplaceTempView("orders")
    orderdetailDF.createOrReplaceTempView("orderdetails")

    sqlOutputDF = spark.sql(sqlQuery)
    # sqlOutputDF.write.format("parquet").mode("overwrite").saveAsTable("retaildb.orders")

    sqlDF = spark.table("retaildb.orders")
    sqlDF.show(5)

    spark.stop()