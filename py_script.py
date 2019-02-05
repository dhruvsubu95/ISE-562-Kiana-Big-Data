import os
import json
import pandas

## Using Spark

from pyspark.sql import SparkSession
spark = SparkSession \
	.builder \
	.appName("kiana analytics") \
	.config("spark.some.config.option", "some-value") \
	.getOrCreate()

path = '/Users/danielgiraldo/Documents/Classes/Spring_2019/ISE_599/Kiana_Analytics'

# create dataframe
d0 = spark.read.json(path)

# data cleaning

d0.drop('_corrupt_record')


d0.printSchema()

# different stores
d0.select('Store').distinct().show() 
d0.crosstab('Store','Level').show()

