import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from dbConfig import get_db_config

def getDataframeFromCSVFileWithHeader(sparksession, filePath, fileSchema = None, filedelim=","):
    reader = sparksession.read \
                .option("header", "true") \
                .option("sep", filedelim) \
                .option("nullValue", "null")

    # If a schema is provided, use it
    if fileSchema:
        reader = reader.schema(fileSchema)
    else:
        # Infer schema automatically
        reader = reader.option("inferSchema", "true")
    
    outputDF = reader.csv(filePath)
    return outputDF

def read_from_db(sparkSession, table_name, jdbc_url):
   df = sparkSession.read \
        .format("jdbc") \
        .option("driver", db['driver']) \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", db['user']) \
        .option("password", db['password']) \
        .option("encrypt", db['encrypt']) \
        .option("trustServerCertificate", db['trust_cert']) \
        .load()
   return df

def write_to_db(df, table_name, jdbc_url):
        df.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", jdbc_url) \
        .option("driver", db['driver']) \
        .option("dbtable", table_name) \
        .option("user", db['user']) \
        .option("password", db['password']) \
        .option("encrypt", db['encrypt']) \
        .option("trustServerCertificate", db['trust_cert']) \
        .save()

if __name__ == '__main__':
    spark = SparkSession.builder \
            .master("local[*]") \
            .appName("Database Ingestion") \
            .config("spark.jars", "D:\\pyspark-training\\.venv\\Lib\\site-packages\\pyspark\\jars\\mssql-jdbc-13.2.1.jre11.jar") \
            .getOrCreate()
    
    db = get_db_config()
    jdbc_url = (
    f"jdbc:sqlserver://{db['host']}:{db['port']};"
    f"databaseName={db['database']}"
)
    logFileLoc = "D:\\pyspark-training\\data\\log_file.txt"
    logFileSchema = ("""date DATE,
                        time TIMESTAMP,
                        size INT,
                        r_version STRING,
                        r_arch STRING,
                        r_os STRING,package STRING,
                        version STRING,
                        country STRING,
                         ip_id INT""")
    logFileDF = (spark.read
                .option("header", "true")
                .option("sep", ",")
                .schema(logFileSchema)
                .csv(logFileLoc)
)
    orderFileLoc = "D:\\pyspark-training\\data\\mongodb-classicmodels-master\\csv\\order.csv"
    ordersSchema = "orderNumber int,orderDate date,requiredDate date,shippedDate date,status string,comments string,customerNumber int"
    ordersDF = getDataframeFromCSVFileWithHeader(spark, orderFileLoc, ordersSchema)
    write_to_db(ordersDF, "dbo.ordersTable", jdbc_url)
    orderTableFromDb = read_from_db(spark, "dbo.ordersTable", jdbc_url)
    orderTableFromDb.show(5)
    logFileFromDb = read_from_db(spark, "dbo.logTable", jdbc_url)
    logFileFromDb.show(5)

        # jdbcDF = spark.read \
    #     .format("jdbc") \
    #     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    #     .option("url", "jdbc:sqlserver://localhost:1433;databaseName=GauravDb") \
    #     .option("dbtable", "retail.Stores") \
    #     .option("user", "pyspark") \
    #     .option("password", "pyspark") \
    #     .option("encrypt", "true") \
    #     .option("trustServerCertificate", "true") \
    #     .load()
    
    # logFileDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url", "jdbc:sqlserver://localhost:1433;databaseName=GauravDb") \
    #     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    #     .option("dbtable", "dbo.logTable") \
    #     .option("user", "pyspark") \
    #     .option("password", "pyspark") \
    #     .option("encrypt", "true") \
    #     .option("trustServerCertificate", "true") \
    #     .save()
    
    # logFileFromDb = spark.read \
    #     .format("jdbc") \
    #     .option("driver", db['driver']) \
    #     .option("url", jdbc_url) \
    #     .option("dbtable", "dbo.logTable") \
    #     .option("user", db['user']) \
    #     .option("password", db['password']) \
    #     .option("encrypt", db['encrypt']) \
    #     .option("trustServerCertificate", db['trust_cert']) \
    #     .load()

    # logFileDF.show(5)

    spark.stop()


