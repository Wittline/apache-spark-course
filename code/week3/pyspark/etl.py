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

    df = spark.read.option("header", "true")\
               .option("multiLine", "true")\
               .schema(end_schema)\
               .csv(input_file)
               .withColumn("date_fixed",to_date(col("Date"), 'MM/dd/yyyy'))

    
    df = df.select([col(c).alias(columns_mapping.get(c, c)) for c in df.columns])

    df = df.withColumn("year",year(col("date_fixed")))\
           .withColumn("month",month(col("date_fixed")))\
           .withColumn("day",dayofmonth(col("date_fixed")))\
           .withColumn('state_bottle_cost_new', regexp_replace('state_bottle_cost', '[$,]', '').cast('float'))\
           .withColumn('state_bottle_retail_new', regexp_replace('state_bottle_retail', '[$,]', '').cast('float'))\
           .withColumn('sale_usd_new', regexp_replace('sale_usd', '[$,]', '').cast('float'))

    df = df.drop("date", "store_location", "state_bottle_cost", "state_bottle_retail", "state_bottle_retail")
    
    df = df.na.fill(-1, subset =["category"])
    
    df = df.na.fill('no_category', subset=["category_name"])

    df.persist(StorageLevel.MEMORY_AND_DISK)

    df.createOrReplaceTempView("ILS")

    db_name = "ILS_db"

    spark.sql(f'CREATE database if not exists {db_name}')

    # Creating dimensions

    # CATEGORY

    dim_category_df = spark.sql(f'''
                select distinct category, category_name from ILS;
         ''')

    
    dim_category_df.write.mode("overwrite").csv(output_path + 'dim_category')

    spark.sql(f'''CREATE TABLE if not exists {db_name}.dim_category
            (
                id INT,
                category_name STRING
            )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{output_path}' 
        TBLPROPERTIES ('skip.header.line.count'='1');
        ''')

    
    # VENDOR

    dim_vendor_df = spark.sql(f'''        
                select distinct vendor_number, vendor_name from ILS;
         ''')

    
    dim_vendor_df.write.mode("overwrite").csv(output_path + 'dim_vendor')

    spark.sql(f'''CREATE TABLE if not exists {db_name}.dim_vendor
            (
                vendor_number INT,
                vendor_name STRING
            )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{output_path}' 
        TBLPROPERTIES ('skip.header.line.count'='1');
        ''')

    # STORE

    dim_store_df = spark.sql(f'''        
                select distinct store_number, store_name, address, city, zip_Code from ILS;
         ''')

    
    dim_store_df.write.mode("overwrite").csv(output_path + 'dim_store')

    spark.sql(f'''CREATE TABLE if not exists {db_name}.dim_store
            (
                store_number INT,
                store_name STRING,
                address STRING,
                city STRING,
                zip_Code STRING
            )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{output_path}' 
        TBLPROPERTIES ('skip.header.line.count'='1');
        ''')


    # COUNTY

    dim_county_df = spark.sql(f'''        
                select distinct county_number, county from ILS;
         ''')

    
    dim_county_df.write.mode("overwrite").csv(output_path + 'dim_county')

    spark.sql(f'''CREATE TABLE if not exists {db_name}.dim_county
            (
                county_number  INT,
                county STRING
            )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{output_path}' 
        TBLPROPERTIES ('skip.header.line.count'='1');
        ''') 


    # ITEMS

    dim_item_df = spark.sql(f'''        
                select distinct item_number, item_description from ILS;
         ''')

    
    dim_item_df.write.mode("overwrite").csv(output_path + 'dim_item')

    spark.sql(f'''CREATE TABLE if not exists {db_name}.dim_item
            (   
                item_number INT, 
                item_description STRING
            )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{output_path}' 
        TBLPROPERTIES ('skip.header.line.count'='1');
        ''')

    
    # DATE

    dim_date_df = spark.sql(f'''   
                select distinct date_fixed, year, month, day from ILS;
         ''')

    
    dim_date_df.write.mode("overwrite").csv(output_path + 'dim_date')

    spark.sql(f'''CREATE TABLE if not exists {db_name}.dim_date
            (
                item_number INT, 
                item_description STRING
            )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{output_path}' 
        TBLPROPERTIES ('skip.header.line.count'='1');
        ''')

    # Creating facts

    # FACT_SALES

    dim_fact_sales_df = spark.sql(f'''   
                select
                    invoice_item_number,
                    date_fixed,
                    bottle_volume_ml,
                    state_bottle_cost_new,
                    state_bottle_retail_new,
                    bottles_sold,
                    sale_usd_new,
                    volume_sold_liters,
                    volume_sold_gallons,                    
                    pack,
                    item_number,
                    vendor_number,
                    category,
                    county_number,
                    store_number
                from ILS;
         ''')

    
    dim_fact_sales_df.write.mode("overwrite").csv(output_path + 'fact_sales')

    spark.sql(f'''CREATE TABLE if not exists {db_name}.fact_sales
            (
                invoice_item_number STRING,
                date_fixed DATE,
                bottle_volume_ml INT,
                state_bottle_cost_new FLOAT,
                state_bottle_retail_new FLOAT,
                bottles_sold INT,
                sale_usd_new FLOAT,
                volume_sold_liters FLOAT,
                volume_sold_gallons FLOAT,                    
                pack INT,
                item_number INT,
                vendor_number INT,
                category INT,
                county_number INT,
                store_number INT
            )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{output_path}' 
        TBLPROPERTIES ('skip.header.line.count'='1');
        ''')
    

    df.unpersist()
    spark.stop()



 