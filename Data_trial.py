#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  5 22:44:58 2019

@author: dhruvsubramaniam
"""

import pandas as pd
import json
df = pd.read_csv('ngs_june1_june4_csv')
import numpy as np
import matplotlib.pyplot as plt

client_mac = np.unique(df['ClientMacAddr'])
clientmac = df['ClientMacAddr']
client_val = client_mac[900]
df2 = df.loc[df['ClientMacAddr']==client_val]
#plt.plot(df2['lat'],df2['lon'])
#plt.plot(df2['lat'],df2['lng'])
plt.scatter(df2['lat'],df2['lng'])
stad_lat = df2['lat']/max(df2['lat'])
stad_lng = df2['lng']/max(df2['lng'])
#plt.scatter(stad_lat,stad_lng)
plt.plot(stad_lat,stad_lng)
plt.plot(df2['lat'],df2['lng'])

df3 = pd.read_json('IRSA_oct1_dec31_json_buenos_aires-000000000099.json',lines=True)
client_mac2 = np.unique(df3['ClientMacAddr'])  
client_val2 = client_mac2[900]
df4 = df3.loc[df3['ClientMacAddr']==client_val2]         

def getplot(df):
    client_mac = np.unique(df['ClientMacAddr'])
    clientmac = df['ClientMacAddr']
    client_val = client_mac[15]
    df2 = df.loc[df['ClientMacAddr']==client_val]   
    stad_lat = df2['lat']/max(df2['lat'])
    stad_lng = df2['lng']/max(df2['lng'])
    plt.plot(stad_lat,stad_lng)
    return(plt.plot(df2['lat'],df2['lng']))
    
date_time_obj = datetime.datetime.strptime(min(df3['localtime']), '%Y-%m-%d %H:%M:%S.%f %Z')
date_time_obj
date_time_obj.time
date_time_obj.date
date_time_obj.day
date_time_obj.year
date_time_obj.hour
date_time_obj.minute
date_time_obj.sec
date_time_obj.second   
df3['date1'] = df3['localtime'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f %Z'))

lat = np.array(df2['lat'])
lng = np.array(df2['lng'])
date = np.array(df2['localtime'])
date = date.tolist()
fig, ax = plt.subplots()
ax.plot(lat, lng)

for i, txt in enumerate(date):
    print([i,txt])
    ax.annotate(txt, (lat[i], lng[i]))
val=[]
for i in client_mac2:
    if len(df3.loc[df3['ClientMacAddr']==i]) > 20:
        val.append(i)
        df4 = df3.loc[df3['ClientMacAddr']==val[100]]
df4 = df4.sort_values(by=['localtime']) 
dict1={} 
for i in client_mac2:
    dict1[i] = np.where(i==df3['ClientMacAddr'])


############ Using Spark ######################
####Initializing
import os
import json
import pandas as pd
import numpy as np
import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
sc = SparkContext()
sqlContext = SQLContext(sc)
###Start Spark Session
spark = SparkSession \
	.builder \
	.appName("kiana analytics") \
	.config("spark.some.config.option", "some-value") \
	.getOrCreate()
#Read File
d0 = spark.read.json('/Users/dhruvsubramaniam/Downloads/IRSA_oct1_dec31_json_buenos_aires-000000000099.json')
d0.drop('_corrupt_record')
d0.printSchema() #Columns
d0.select('Store').distinct().show() 
d0.crosstab('Store','Level').show()
d1 = d0.select('ClientMacAddr').distinct()
d1 = [d0.select('ClientMacAddr').distinct().count(),d0.select('localtime').distinct().count()] #Count unique values
#### Count Based on MacID #######
d8 = d0.groupBy('ClientMacAddr').count()
df2 = d8.filter(d8['count']>20) ###Get MacID appearing more than 20 times
sqlContext.registerDataFrameAsTable(d8,'d8_table')
df2 = sqlContext.sql("SELECT ClientMacAddr FROM d8_table WHERE count>20")### SQL Get MacID appearing more than 20 times
##Cut Based on Date and Time
d5=d0
split_col =pyspark.sql.functions.split(d5['localtime'], ' ')
d5 = d5.withColumn('Date', split_col.getItem(0))
d5 = d5.withColumn('Time', split_col.getItem(1))
sqlContext.registerDataFrameAsTable(d5,'d5_table')
df5 = sqlContext.sql("SELECT * FROM d5_table WHERE Date=='2018-10-30' AND Time>'05:15:33'")

