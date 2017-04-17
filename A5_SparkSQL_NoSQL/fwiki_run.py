#!/usr/bin/env python3
#
# Run this program with spark-submit

import sys,os,datetime,re,operator
from pyspark import SparkContext, SparkConf
from fwiki import LogLine



if __name__=="__main__":
    # Get your sparkcontext and make a dataframe
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("quazyilx").getOrCreate()
    sc    = spark.sparkContext      # get the context
    
    # Create an RDD from s3://gu-anly502/logs/forensicswiki.2012.txt
    url = "s3://gu-anly502/logs/forensicswiki.2012.txt"
    # url = "/Users/Ruhan/repos/anly502_2017_spring/A5/forensicswiki_sub.txt"
    # NOTE: Do this with 1 master m3.xlarge, 2 core m3.xlarge, and 4 task m3.xlarge
    # otherwise it will take forever...
    
    loglines = sc.textFile(url).cache()
    logs     = loglines.map(lambda l: LogLine(l).row()).cache()
    df       = spark.createDataFrame(logs).cache()

    df.createOrReplaceTempView("logs")
    
    # Register the dataframe as an SQL table called 'logs'

    # Print how many log lines there are
    print("Total Log Lines: {}".format(spark.sql("select count(*) from logs")))

    # Figure out when it started and ended
    (start,end) = spark.sql("select min(datetime),max(datetime) from logs").collect()[0]

    print("Date range: {} to {}".format(start,end))

    # Now generate the requested output
    query_result = spark.sql("select substr(datetime,1,7) as Date,count(1) as num from logs GROUP BY 1 ORDER BY substr(datetime,1,7)").collect()

    # print out result
    with open('fwiki_run.txt','w') as f:

        for row in query_result:
            if row.Date == None: # remove none line
                continue
            print("%s %d "%(row.Date,row.num))
            f.write("%s %d\n"%(row.Date,row.num))






