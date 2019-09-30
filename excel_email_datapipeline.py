from com.crealytics.spark.excel import *

## using spark-submit with option to execute script from command line
## spark-submit --packages spark-excel_2.11:0.11.1 excel_email_datapipeline.py

## pyspark --packages spark-excel_2.11:0.11.1
# pyspark --packages com.crealytics:spark-excel_2.11:0.11.1

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("excel-email-pipeline").getOrCreate()

# df = spark.read.format("com.crealytics.spark.excel").load("data/excel.xlsx")


# df = spark.read.format("com.crealytics.spark.excel").option("useHeader", "true").option("maxRowsInMemory", 20).option("excerptSize", 10).load("data/excel.xlsx")


df = spark.read.format("com.crealytics.spark.excel").option("useHeader", "true").option("inferSchema", "true").load("data/excel.xlsx")

df.show()
