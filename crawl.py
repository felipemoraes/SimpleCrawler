from lxml import html
from lxml import etree
from bs4 import UnicodeDammit
import chardet 
import requests
import argparse
from lxml.html.clean import Cleaner
from elasticsearch import Elasticsearch, helpers
import threading
import datetime
import time
import logging
from bs4 import BeautifulSoup,SoupStrainer,Comment

__author__ = 'Felipe Moraes'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def visible(element):
    if element.parent.name in ['style', 'script']:
        return False
    return True

def get_args():

	# Assign description to the help doc
	parser = argparse.ArgumentParser(description='Script to collect a list of urls.')
	# Add arguments
	parser.add_argument(
		'-s', '--seed', help='Seed file', required=True)
	parser.add_argument(
		'-i', '--index', help='Elasticsearch index name', required=True)    
	# Array for all arguments passed to script
	args = parser.parse_args()
	# Assign args to variables
	seed = args.seed
	index = args.index
	# Return all variable values
	return seed, index

class Fetch (threading.Thread):

	def __init__(self, url):
		threading.Thread.__init__(self)
		self.url = url
		self.doc = None

	def run(self):
		try:
			page = requests.get(self.url)
		except ConnectionError:
			logger.error("Failed to connect")
			return
		ud = UnicodeDammit(page.content, is_html=True)

		enc = ud.original_encoding.lower()
		declared_enc = ud.declared_html_encoding
		if declared_enc:
			declared_enc = declared_enc.lower()
			# possible misregocnition of an encoding
		if (declared_enc and enc != declared_enc):
			detect_dict = chardet.detect(page.content)
			det_conf = detect_dict["confidence"]
			det_enc = detect_dict["encoding"].lower()
			if enc == det_enc and det_conf < 0.99:
				enc = declared_enc
		# if page contains any characters that differ from the main
		# encodin we will ignore them
		ts = time.time()
		st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
		try:
			content = page.content.decode(enc, "ignore").encode(enc)
			htmlparser = etree.HTMLParser(encoding=enc)
			root = etree.HTML(content, parser=htmlparser)
			descs = root.xpath('//meta[re:test(@name, "^description$", "i")]', namespaces={"re": "http://exslt.org/regular-expressions"})
			description = ''.join(desc.get('content') for desc in descs if desc is not None and desc.get('content') is not None)
			title = root.findtext('.//title')
			etree.strip_elements(root, html.etree.Comment, "script", "style")
			text = html.tostring(root, method="text", encoding=unicode)
		except:
			self.doc = { 'url' : self.url,'html': page.text, 'timestamp': st}
			logger.error("Failed to parse html")
			return

		self.doc = {'description': description, 'title' : title, 'url' : self.url, 'text': text, 'html': page.text, 'timestamp': st}
		logger.info('Fetched %s url' % self.url)



def main():
	logger.info('Starting crawler')
	seedfile, index = get_args()
	es = Elasticsearch()
	f = open("crawled_urls.txt", "r")
	crawled_urls = set([line.strip() for line in f])
	f.close()
	f = open("crawled_urls.txt", "a")
	bulk_data = []
	seeds = []
	for line in open(seedfile):
		if line.strip() not in crawled_urls:
			seeds.append(line.strip())
	logger.info('Crawling %d urls' % len(seeds))

	threads = []

	for url in seeds:
		thread = Fetch(url)
		thread.start()
		threads.append(thread)
		if len(threads) == 20:
			for thread in threads:
				 thread.join()

			for thread in threads:
				doc = thread.doc
				if doc:
					doc['_type'] = index
					doc['_index'] = index
					doc['_id'] = doc['url']
					bulk_data.append(doc)
					

			if len(bulk_data) > 100:
				logger.info('Sending bulk data to Elasticsearch')
				try:
					helpers.bulk(es,bulk_data)
					for doc in bulk_data:
						f.write(doc['url'] + "\n")
					bulk_data = []
				except:
					logger.error("Failed to send data to Elasticsearch")

			threads = []

	if len(threads) > 0:
		for thread in threads:
			doc = thread.doc
			doc['_type'] = index
			doc['_index'] = index
			doc['_id'] = doc['url']
			bulk_data.append(doc)
			f.write(doc['url'] + "\n")

	helpers.bulk(es,bulk_data)
	for doc in bulk_data:
		f.write(doc['url'] + "\n")
	logger.info('Sending bulk data to Elasticsearch')

if __name__ == "__main__":
	main()