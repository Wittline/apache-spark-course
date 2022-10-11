import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("mycoursespark").getOrCreate()

schema = StructType() \
    .add("col1", StringType(), True)\
    .add("col2", IntegerType(), True)\
    .add("col3", IntegerType(), True)

df_schema = spark.read.format("csv") \
    .option("header", True) \
    .schema(schema) \
    .load("/opt/spark/test.csv")

df_schema.printSchema()

df_schema_name_columns = df_schema.withColumnRenamed("col1", "item") \
                        .withColumnRenamed("col2", "amount") \
                        .withColumnRenamed("col3", "year")


df_schema_name_columns.printSchema()

df_schema_name_columns_2 = df_schema_name_columns.select(col("item"), col("amount"))

df_schema_name_columns_2.printSchema()

df_schema_name_columns_2.write.options(header='True', delimiter=',') \
    .csv("/opt/spark/test_schema")










