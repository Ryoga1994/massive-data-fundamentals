import sys
import os,datetime,re

# We are giving you the regular expression!

line = '77.21.0.59 - - [01/Jan/2012:00:35:03 -0800] "GET /wiki/Write_Blockers HTTP/1.1" 200 5742 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/534.52.7 (KHTML, like Gecko) Version/5.1.2 Safari/534.52.7"'

# CLR_RE = re.compile(r'^(\S+) (\S+) (\S+) \[([^\]]+)\] "(\S+) (\S+) \S+" (\S+) (\S+) "([^"]*)" "([^"]*)"')
CLR_RE = re.compile(r'^(\S+) (\S+) (\S+) \[([^\]]+)\] "(\S+) (.+) \S+" (\S+) (\S+) "([^"]*)" "([^"]*)"')
# CLR_RE = re.compile(r'(\S+) (\S+) (\S+) \[([^\]]+)\] " *(\S+) (.+?) \S+ *" (\S+) (\S+) "(.*?)" "(.*?)"')

log_re = re.compile(CLR_RE)

class LogLine():
    __slots__ = ['ipaddr','path','method','datetime','result','bytes','refer','agent']
    def __init__(self,line):
        # Parse line with the regular expression
        m = log_re.search(line)
        if not m:
            self.ipaddr   = None
            self.method   = None
            self.path     = None
            self.datetime = None
            self.result   = None
            self.bytes    = None
            self.refer    = None
            self.agent    = None
            return

        self.ipaddr     = m.group(1)
        self.datetime   = datetime.datetime.strptime(m.group(4),"%d/%b/%Y:%H:%M:%S %z")
        # You need to fill in the rest...
        self.method = m.group(5)
        self.path = m.group(6)
        self.result = int(m.group(7))
        self.bytes = int(m.group(8))
        self.refer = m.group(9)
        self.agent = m.group(10)

    def __str__(self):
        return "{} {} {} {} {} {} {} {}".format(self.ipaddr,self.method,self.path,self.datetime.isoformat(),self.result,self.bytes,self.refer,self.agent)

    def __repr__(self):
        return "<LogLine: {} {} {} {} {} {} {} {}>".format(self.ipaddr,self.method,self.path,self.datetime.isoformat(),self.result,self.bytes,self.refer,self.agent)

    def row(self):
        from pyspark.sql import Row
        # You need to fill in the rest:
        return Row(ipaddr=self.ipaddr,datetime=self.datetime)



def main():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("logs").getOrCreate()
    sc = spark.sparkContext  # get the context

    # Replace this code with your own
    lines = sc.textFile("s3://gu-anly502/logs/forensicswiki.2012.txt").cache()
    # lines = sc.textFile("/Users/Ruhan/repos/anly502_2017_spring/A5/forensicswiki.2012.txt")

    rows = lines.map(lambda l: LogLine(l).Row())

    # register your dataframe as the SQL table quazyilx
    # You probably want to cache it, also!
    schemaDF = spark.createDataFrame(rows).cache()
    schemaDF.collect()
    schemaDF.createOrReplaceTempView("logs")













