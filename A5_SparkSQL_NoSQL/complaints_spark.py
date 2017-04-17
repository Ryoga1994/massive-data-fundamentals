# run on Apache SparkSQL, by
#
# loading the dataset into a table and
#
# computing the result with an SQL query


class Splitter():

    def __init__(self,line):
        import datetime
        self.id = line[1]
        self.datetime_receive = datetime.datetime.strptime(line[8],"%Y-%m-%dT%H:%M:%S") # 'The date the CFPB received the complaint'
        self.datetime_sent = datetime.datetime.strptime(line[21],"%Y-%m-%dT%H:%M:%S")

    def Row(self):
        from pyspark.sql import Row
        return Row(id = self.id,datetime_receive = self.datetime_receive,datetime_sent = self.datetime_sent)


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark import SparkContext, SparkConf

    import json
    import urllib
    import requests

    spark = SparkSession.builder.appName("rows").getOrCreate()
    sc = spark.sparkContext  # get the context

    data = json.loads(urllib.request.urlopen("http://data.consumerfinance.gov/api/views/7zpz-7ury/rows.json").read().decode('utf-8'))
    events = sc.parallelize(data['data'])


    # data['meta']['view']['columns'][1]

    rows = events.map(lambda l: Splitter(l).Row()).cache()

    schemaDF = spark.createDataFrame(rows)
    schemaDF.createOrReplaceTempView("complaints")

    query_result = spark.sql("SELECT SUBSTR(datetime_receive,1,4) as Year ,count(id) as num FROM complaints "
                             "GROUP BY SUBSTR(datetime_receive,1,4) ORDER BY SUBSTR(datetime_receive,1,4)").collect()
                             
    # print(query_result)
    with open("complaints_spark.txt",'w') as f:

        for row in query_result:
            print("%s %d"%(row.Year,row.num))
            f.write("%s %d\n" % (row.Year, row.num))
