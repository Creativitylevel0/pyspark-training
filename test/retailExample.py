from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("pySpark Application").getOrCreate()
    sc = spark.sparkContext

    orderdetailLoc = "D:\\pyspark-training\\data\\mongodb-classicmodels-master\\csv\\orderdetail.csv"
    orderdetailRDD = sc.textFile(orderdetailLoc)
    # find the total sales amount for each product code
    # header : orderNumber,productCode,quantityOrdered,priceEach,orderLineNumber
    print(orderdetailRDD.take(10))
    odheader = orderdetailRDD.first()
    orderdetailRDD1 = orderdetailRDD.filter(lambda x: x != odheader)
    print(orderdetailRDD1.take(10))
    orderdetailRDD2 = orderdetailRDD1.map(lambda x: x.split(","))
    print(orderdetailRDD2.take(10))
    orderdetailRDD3 = orderdetailRDD2.map(lambda x: (x[1], int(x[2]) * float(x[3])))
    print(orderdetailRDD3.take(10))
    orderdetailRDD4 = orderdetailRDD3.reduceByKey(lambda acc, value: acc + value)
    print(orderdetailRDD4.take(10))
    orderdetailRDD5 = orderdetailRDD4.mapValues(lambda x: round(x))
    print(orderdetailRDD5.take(10))
    #orderdetailRDD5.saveAsTextFile("/home/azureadmin/training/output/prodsales")

    spark.stop()