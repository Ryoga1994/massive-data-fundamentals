import pytest
from fwiki import *

# Sample weblog entry
LOG0='77.21.0.59 - - [24/Jan/2012:00:35:04 -0800] "GET /w/skins/common/wikibits.js?270 HTTP/1.1" 200 31165 "http://www.forensicswiki.org/wiki/Write_Blockers" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/534.52.7 (KHTML, like Gecko) Version/5.1.2 Safari/534.52.7"'
LOG1='72.35.92.170 - - [01/Jan/2012:02:08:41 -0800] "GET /\" + gaJsHost + \"google-analytics.com/ga.js HTTP/1.1" 301 517 "-" "Java/1.5.0_09"'

# CLR_RE = re.compile(r'^(\S+) (\S+) (\S+) \[([^\]]+)\] "(\S+) (\S+) \S+" (\S+) (\S+) "([^"]*)" "([^"]*)"')

# CLR_RE = re.compile(r'(\S+) (\S+) (\S+) \[(.+)\] \"(\S+) (.+) \S+\" (\d+) (\d+) \"([^\"]*)\" \"([^\"]*)\"') # Updated


log_re = re.compile(CLR_RE)

# create a test
def test_CLR_RE():
    m = CLR_RE.search(LOG1)
    assert m.group(1)=="77.21.0.59"
    assert m.group(2)=="-"
    assert m.group(3)=="-"
    assert m.group(4)=="24/Jan/2012:00:35:04 -0800"
    assert m.group(5)=="GET"
    assert m.group(6)=="/w/skins/common/wikibits.js?270"
    assert m.group(7)=="200"
    assert m.group(8)=="31165"
    assert m.group(9)=="http://www.forensicswiki.org/wiki/Write_Blockers"
    assert m.group(10)=="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/534.52.7 (KHTML, like Gecko) Version/5.1.2 Safari/534.52.7"

def test_LogLine():
    log = LogLine(LOG1)
    assert log.ipaddr == '77.21.0.59'
    assert log.method == 'GET'
    assert log.path == '/w/skins/common/wikibits.js?270'
    assert log.datetime.isoformat() == '2012-01-24T00:35:04-08:00'
    assert log.result == 200
    assert log.bytes == 31165
    assert log.refer == 'http://www.forensicswiki.org/wiki/Write_Blockers'
    assert log.agent == 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/534.52.7 (KHTML, like Gecko) Version/5.1.2 Safari/534.52.7'

    log = LogLine(LOG1)
    assert log.ipaddr == '72.35.92.170'
    assert log.method == 'GET'
    assert log.path == '/\" + gaJsHost + \"google-analytics.com/ga.js'
    assert log.datetime.isoformat() == '2012-01-01T02:08:41-08:00'
    assert log.result == 301
    assert log.bytes == 517
    assert log.refer == '-'
    assert log.agent == 'Java/1.5.0_09'


