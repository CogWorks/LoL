#!/usr/bin/env python2 -tt

from lxml.html import fromstring, tostring
import sys, os
import time
import json
import codecs

from PyQt4.QtGui import *
from PyQt4.QtCore import *
from PyQt4.QtWebKit import *

class Scraper(QWebPage):
	def __init__(self, urlprefix, max_pages, filename):
		if os.path.exists(filename):
			print "Error: %s already exists!" % filename
			sys.exit(-1)
		self.app = QApplication(sys.argv)
		QWebPage.__init__(self)
		self.urlprefix = urlprefix
		self.max_pages = max_pages
		self.page = 0
		self.file = open(filename, 'a')
		self.loadFinished.connect(self._loadFinished)
		self.scrape()
		self.app.exec_()

	def scrape(self):
		if self.page < self.max_pages:
			self.page += 1
			out = "-> On page {} of {}....      {}%"
			print out.format(self.page, self.max_pages, str(round(float(self.page)/self.max_pages*100, 2)))
			self.mainFrame().setUrl(QUrl(self.urlprefix + str(self.page) + "/"))
		else:
			self.file.close()
			self.app.quit()

	def _loadFinished(self, result):
		html = str(self.mainFrame().toHtml().toUtf8())
		dom = fromstring(html)
		sels = dom.xpath('//*[(@id = "leaderboard_table")]//td[(((count(preceding-sibling::*) + 1) = 4) and parent::*)]//a')
		for s in sels:
			self.file.write(s.get('href').split('/')[3]+'\n')
		self.file.flush()
		self.scrape()

s = Scraper("http://www.lolking.net/leaderboards/#/na/", 50000, "summoner_ids_na.txt")