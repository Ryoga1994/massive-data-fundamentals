# Ruhan Wang (rw848)

# q1.py

# create sc instance
from pyspark import SparkContext

sc = SparkContext.getOrCreate()

top1m = sc.textFile("/top-1m.csv").cache()

# find record ends with ".com"
domain_com = top1m.filter(lambda  _str: _str.endswith('.com'))
domain_com.cache()

num_count = domain_com.count()
# write to q2.txt

con = open("q2.txt",'w')
con.write(str(num_count)+"\n")
con.close()

