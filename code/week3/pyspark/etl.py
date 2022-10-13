import os
import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType


#s3://racv-emr-serverless-bucket/datasets/test.txt

if __name__ == "__main__":

    input_file = sys.argv[1]


    spark = SparkSession\
        .builder\
        .appName("ETL_Datawarehuse")\
        .enableHiveSupport()\
        .getOrCreate()


    schema = [ 
        StructField("sales", IntegerType(), True), 
        StructField("name", StringType(), True)
        ]

    end_schema = StructType(fields = schema)

    df = spark.read.json(input_file, schema = end_schema)

    df.persist()

    


    


        
    spark.stop()