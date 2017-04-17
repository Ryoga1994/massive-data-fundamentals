from subprocess import Popen, PIPE

# url = 'stackoverflow.com'

# page = get_url(url)

def google_analytics(url):
    try:
        page = Popen(['curl','-s',url],stdout=PIPE).communicate()[0].decode('utf-8','ignore')
    except:
        print("The page is not in ASCII or UTF-8.")

    return ('analytics.js' in page) or ('ga.js' in page)



# main()
from pyspark import SparkContext

sc = SparkContext.getOrCreate()

top1m = sc.textFile("/top-1m.csv").cache()

top1k = top1m.take(1000)

# write to file top1k_analytics.txt

con = open('top1k_analytics.txt','w')

for line in top1k:
    url = line.split(',')[1].strip()

    if google_analytics(url):
        con.write("%s\n"%url)

con.close()





