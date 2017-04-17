#!/usr/bin/env python3
#
# Run this program with spark-submit

import sys,os,datetime,re,operator
from pyspark import SparkContext, SparkConf
from quazyilx import Quazyilx

INSERT_YOUR_CODE_HERE = ""

QUERIES = [["total_rows","select count(*) from quazyilx"],
           ["total_errors","select count(*) from quazyilx where fnard=-1 and fnok = -1 and cark = -1 and gnuck = -1"], # fnard==fnork==cark==-1
           ["one_error_others_gt5","select count(*) from quazyilx where fnard=-1 and fnok > 5 and cark >5"], # fnard==-1, fnok>5
           ["first_date","select min(datetime) from quazyilx"],
           ["last_date","select max(datetime) from quazyilx"],
           ["first_error_date","select min(datetime) from quazyilx where fnard=-1 and fnok = -1 and cark = -1 and gnuck = -1"],
           ["last_error_date","select max(datetime) from quazyilx where fnard=-1 and fnok = -1 and cark = -1 and gnuck = -1"]
]



if __name__=="__main__":
    # Get your sparkcontext and make a dataframe
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("quazyilx1").getOrCreate()
    sc    = spark.sparkContext      # get the context
    
    # Replace this code with your own
    lines = sc.textFile("s3://gu-anly502/A1/quazyilx1.txt")
    # lines = sc.textFile("/Users/Ruhan/repos/anly502_2017_spring/A5/quazyilx0.txt")

    print("*** Verifying that Spark works ***",file=sys.stderr)
    res = sc.parallelize(range(1,1000)).reduce(operator.add)
    print("*** Result = {}  (should be 499500) ***".format(res),file=sys.stderr)
    assert res==499500

    # Create an RDD from s3://gu-anly502/A1/quazyilx1.txt
    # NOTE: Do this with 1 master m3.xlarge, 2 core m3.xlarge, and 4 task m3.xlarge
    # otherwise it will take forever...

    rows = lines.map(lambda l: Quazyilx(l).Row())
    
    # register your dataframe as the SQL table quazyilx
    # You probably want to cache it, also!
    schemaDF = spark.createDataFrame(rows).cache()
    schemaDF.createOrReplaceTempView("quazyilx")

    # Print how many rows we have
    print("rows: {}".format(spark.sql("select count(*) from quazyilx").collect()))

    # test queries
    #spark.sql("select count(*) from quazyilx where fnard=-1 and fnok = -1 and cark = -1 and gnuck = -1").collect()
    #spark.sql("select count(*) from quazyilx where fnard=-1 and fnok > 5 and cark >5").collect()
    #spark.sql("select min(datetime) from quazyilx where fnard=-1 and fnok = -1 and cark = -1 and gnuck = -1").collect()
    #spark.sql("select max(datetime) from quazyilx where fnard=-1 and fnok = -1 and cark = -1 and gnuck = -1").collect()
    # Now do the queries

    # write into quazyilx_run.txt
    con = open("quazyilx_run.txt",'w')

    for (var,query) in QUERIES:
        print("{}-query: {}".format(var,query))

        con.write("{}-query: {}\n".format(var,query))

        if query:
            query_result = spark.sql(query).collect()
            print("{}: {}".format(var,query_result))
            con.write("{}: {}\n".format(var,query_result))

    con.close()
