

def google_analytics(url):
    """return true when url using google analytics"""
    from subprocess import Popen, PIPE
    try:
        page = Popen(['curl','-s',url],stdout=PIPE).communicate()[0].decode('utf-8','ignore')
    except:
        print("The page is not in ASCII or UTF-8.")

    return ('analytics.js' in page) or ('ga.js' in page)