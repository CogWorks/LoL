#!/usr/bin/env python2 -tt

from lxml.html import fromstring, tostring
import sys, os
import time
import json
import codecs

import urllib2
from lxml.html import fromstring, tostring
import sys
import time
import json
import codecs

filename = "summoner_names_kr.txt"
urlprefix = "http://lol.inven.co.kr/dataninfo/ladder/index.php?pg="
max_pages = 35000

if os.path.exists(filename):
	print "Error: %s already exists!" % filename
	sys.exit(-1)
f = codecs.open(filename, 'a' , 'utf-8')

for page in xrange(1, max_pages+1):
    out = "-> On page {} of {}....      {}%"
    print out.format(page, max_pages, str(round(float(page)/max_pages*100, 2)))
    response = urllib2.urlopen(urlprefix + str(page) + "/")
    html = response.read()
    dom = fromstring(html)
    sels = dom.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "ladderTable", " " ))]//a')
    for s in sels:
    	if s.text != None:
    		f.write(s.text+'\n')
    f.flush()

f.close()