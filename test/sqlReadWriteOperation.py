import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from dbConfig import get_db_config


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
                        r_version FLOAT,
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
    #     .option("dbtable", "logTable") \
    #     .option("user", "pyspark") \
    #     .option("password", "pyspark") \
    #     .option("encrypt", "true") \
    #     .option("trustServerCertificate", "true") \
    #     .save()
    
    logFileFromDb = spark.read \
        .format("jdbc") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("url", jdbc_url) \
        .option("dbtable", "dbo.logTable") \
        .option("user", db['user']) \
        .option("password", db['password']) \
        .option("encrypt", db['encrypt']) \
        .option("trustServerCertificate", db['trust_cert']) \
        .load()
    
    logFileFromDb.show(5)

    # logFileDF.show(5)

    spark.stop()


