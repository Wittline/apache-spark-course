import os
import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


#s3://racv-emr-serverless-bucket/datasets/test.txt, s3://racv-emr-serverless-bucket/outputs/wordcount.csv
if __name__ == "__main__":

    if (len(sys.argv) != 3):
        print("Wordcount: [input-file] [output-path]")
        sys.exit(0)
    

    input_file = sys.argv[1]
    output_path = sys.argv[2]


    spark = SparkSession\
        .builder\
        .appName("WordCount")\
        .getOrCreate()

    
    text_file = spark.sparkContext.textFile(input_file)

    counts = text_file.flatMap(lambda line: line.split(" "))\
                      .map(lambda word: (word, 1))\
                      .reduceByKey(lambda a, b: a + b)\
                      .sortBy(lambda x: x[1], False)

    counts_df = counts.toDF(["word","count"])

    
    counts_df.write.mode("overwrite").csv(output_path)
        
    spark.stop()