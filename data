import os
import json
import pandas

from pyspark.sql import SparkSession
spark = SparkSession \
	.builder \
	.appName("kiana analytics") \
	.config("spark.some.config.option", "some-value") \
	.getOrCreate()

path = '/Users/danielgiraldo/Documents/Classes/Spring_2019/ISE_599/Kiana_Analytics'

df = spark.read.json(path)
