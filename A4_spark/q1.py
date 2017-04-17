# Ruhan Wang (rw848)

# q1.py

# create sc instance
from pyspark import SparkContext

sc = SparkContext.getOrCreate()

top1m = sc.textFile("/top-1m.csv").cache()

num_count = top1m.count()

# write into q1.txt
con = open("q1.txt",'w')
con.write(str(num_count)+"\n")
con.close()



