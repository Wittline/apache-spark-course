from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, FloatType, DecimalType, DateType, LongType
from pyspark.sql.functions import to_date,col, dayofmonth, month, year, weekofyear, format_number, date_format, regexp_replace
from pyspark import StorageLevel
import os
import sys


["s3://racv-emr-bucket/dataset/Iowa_Liquor_Sales.csv","s3://racv-emr-bucket/dataset/"]

if __name__ == "__main__":

    input_file = sys.argv[1]
    output_path = sys.argv[2]


    spark = SparkSession\
        .builder\
        .appName("etl_project")\
        .enableHiveSupport()\
        .getOrCreate()
      

    columns_mapping= {    
        'Invoice/Item Number':'invoice_item_number',
        'Date':'date',
        'Store Number':'store_number',
        'Store Name':'store_name',
        'Address':'address',
        'City':'city',
        'Zip Code':'zip_Code' ,
        'Store Location':'store_location',
        'County Number': 'county_number',
        'County':'county',
        'Category':'category',
        'Category Name':'category_name',
        'Vendor Number':'vendor_number',
        'Vendor Name':'vendor_name',
        'Item Number': 'item_number',
        'Item Description':'item_description',
        'Pack': 'pack',
        'Bottle Volume (ml)': 'bottle_volume_ml',
        'State Bottle Cost':'state_bottle_cost',
        'State Bottle Retail':'state_bottle_retail',
        'Bottles Sold':'bottles_sold',
        'Sale (Dollars)':'sale_usd',
        'Volume Sold (Liters)': 'volume_sold_liters',
        'Volume Sold (Gallons)': 'volume_sold_gallons'
    }

    schema = [
        StructField('Invoice/Item Number', StringType(), True),
        StructField('Date', StringType(), True),
        StructField('Store Number', IntegerType(), True),
        StructField('Store Name', StringType(), True),
        StructField('Address', StringType(), True),
        StructField('City', StringType(), True),
        StructField('Zip Code', StringType(), True),
        StructField('Store Location', StringType(), True),
        StructField('County Number', IntegerType(), True),
        StructField('County', StringType(), True),
        StructField('Category', IntegerType(), True),
        StructField('Category Name', StringType(), True),
        StructField('Vendor Number', IntegerType(), True),
        StructField('Vendor Name', StringType(), True),
        StructField('Item Number', IntegerType(), True),
        StructField('Item Description', StringType(), True),
        StructField('Pack', IntegerType(), True),
        StructField('Bottle Volume (ml)', IntegerType(), True),
        StructField('State Bottle Cost', StringType(), True),
        StructField('State Bottle Retail', StringType(), True),
        StructField('Bottles Sold', IntegerType(), True),
        StructField('Sale (Dollars)', StringType(), True),
        StructField('Volume Sold (Liters)', DoubleType(), True),
        StructField("Volume Sold (Gallons)", DoubleType(), True)
    ]

    end_schema = StructType(fields = schema)
