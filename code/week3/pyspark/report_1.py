
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, FloatType, DecimalType, DateType, LongType
from pyspark.sql.functions import to_date,col, dayofmonth, month, year, weekofyear, format_number, date_format, regexp_replace
from pyspark import StorageLevel
import os
import sys


#["s3://racv-emr-bucket/dataset/"]

if __name__ == "__main__":

    year = sys.argv[1]

    spark = (
        SparkSession.builder.appName("SparkSQL")
        .config("mapred.input.dir.recursive", "true")   
        .config("spark.hive.mapred.supports.subdirectories","true")
        .config("mapreduce.input.fileinputformat.input.dir.recursive","true")
        .config(
            "hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        )
        .enableHiveSupport()
        .getOrCreate()
    )


    spark.catalog.setCurrentDatabase("ILS_db")

    df = spark.sql("""
            SELECT sum(f.sale_usd_new),  c.category_name
            FROM fact_sales f
            INNER JOIN dim_category c
            ON c.id = f.category
            group by c.category_name
            order by 1 desc            
            limit 20   
    """)

    df.write.mode("overwrite").csv(f"s3://racv-emr-bucket/reports/myresult.csv")

    spark.stop()


