#
# Copyright 2013 Metamarkets Group Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import division
import urllib2
import simplejson as json
import csv
import re
import os
import sys
import pandas
import dateutil.parser
from matplotlib import *
from matplotlib.pyplot import *
from pyDruidUtils.aggregators import *
from pyDruidUtils.postaggregator import *
from pyDruidUtils.filters import *
from pyDruidUtils.query_utils import *


class pyDruid:

	def __init__(self,url,endpoint):
		self.url = url
		self.endpoint = endpoint
		self.result = None
		self.result_json = 	None
		self.query_type = None

	# serializes a dict representation of the query into a json
	# object, and sends it to bard via post, and gets back the
	# json representation of the query result
	def post(self,query):
		querystr = json.dumps(query)
		url = self.url + '/' + self.endpoint
		headers = {'Content-Type' : 'application/json'}
		req = urllib2.Request(url, querystr, headers)
		res = urllib2.urlopen(req)
		data = res.read()
		self.result_json = data;
		self.querystr = querystr
		res.close()

	# de-serializes the data returned from druid into a
	# list of dicts
	def parse(self):
		if self.result_json:
			res = json.loads(self.result_json)
			return res
		else:
			print("Empty query")
			return None

	def export_tsv(self,dest_path):

		f = open(dest_path,'wb')
		tsv_file = csv.writer(f, delimiter = '\t')

		if(self.query_type == "timeseries"):
			header = self.result[0]['result'].keys()
			header.append('timestamp')
		elif(self.query_type == "groupby"):
			header = self.result[0]['event'].keys()
			header.append('timestamp')
			header.append('version')

		tsv_file.writerow(header)

		# use unicodewriter to encode results in unicode 
		w = query_utils.UnicodeWriter(f)

		if self.result:
			if self.query_type == "timeseries":
				for item in self.result:
					timestamp = item['timestamp']
					result = item['result']

					if type(result) is list: 
						for line in result:
							w.writerow(line.values() + [timestamp])
					else: #timeseries
						w.writerow(result.values() + [timestamp])
			elif self.query_type == "groupby":
				for item in self.result:
					timestamp = item['timestamp']
					version = item['version']
					w.writerow(item['event'].values() + [timestamp] + [version])

		f.close()

	# Exports a JSON query object into a Pandas data-frame
	def export_pandas(self): 
		if self.result:
			if self.query_type == "timeseries":
				nres = [v['result'].items() + [('timestamp',v['timestamp'])] for v in self.result]
				nres = [dict(v) for v in nres]
			else:
				print('not implemented yet')
				return None

			df = pandas.DataFrame(nres)
			df['timestamp'] = df['timestamp'].map(lambda x: dateutil.parser.parse(x))
			df['t'] = dates.date2num(df['timestamp'])
			return df	

	# implements a timeseries query
	def timeseries(self, **args): 
		
		query_dict = {"queryType" : "timeseries"}
		valid_parts = ['dataSource', 'granularity', 'filter', 'aggregations', 'postAggregations', 'intervals']

		for key, val in args.iteritems():
			if key not in valid_parts:
				raise ValueError('{0} is not a valid query component. The list of valid components is: \n {1}'.format(key, valid_parts))
			elif key == "aggregations" :
				query_dict[key] = build_aggregators(val)
			elif key == "postAggregations":
				query_dict[key] = build_post_aggregators(val)
			elif key == "filter":
				query_dict[key] = val.filter['filter']
			else:
				query_dict[key] = val

		self.query_dict = query_dict

		try:
			self.post(query_dict)
		except urllib2.HTTPError, e:
			raise IOError('Malformed query: \n {0}'.format(json.dumps(self.query_dict ,indent = 4)))
		else:
			self.result = self.parse()
			self.query_type = "timeseries"
			return self.result

	# Implements a groupBy query
	def groupBy(self, **args): 
		
		query_dict = {"queryType" : "groupBy"}
		valid_parts = ['dataSource', 'granularity', 'filter', 'aggregations', 'postAggregations',
		   			   'intervals', 'dimensions']

		for key, val in args.iteritems():
			if key not in valid_parts:
				raise ValueError('{0} is not a valid query component. The list of valid components is: \n {1}'.format(key, valid_parts))
			elif key == "aggregations" :
				query_dict[key] = build_aggregators(val)
			elif key == "postAggregations":
				query_dict[key] = build_post_aggregators(val)
			elif key == "filter":
				query_dict[key] = val.filter['filter']
			else:
				query_dict[key] = val

		self.query_dict = query_dict

		try:
			self.post(query_dict)
		except urllib2.HTTPError, e:
			raise IOError('Malformed query: \n {0}'.format(json.dumps(self.query_dict ,indent = 4)))
		else:
			self.result = self.parse()
			self.query_type = "groupby"
			return self.parse()

	# Implements a segmentMetadata query.  This query type is for pulling the tsv-equivalent 
	# size and cardinality broken out by dimension in a specified data source.
	def segmentMetadata(self, **args): 
		
		query_dict = {"queryType" : "segmentMetadata"}
		valid_parts = ['dataSource', 'intervals']

		for key, val in args.iteritems():
			if key not in valid_parts:
				raise ValueError('{0} is not a valid query component. The list of valid components is: \n {1}'.format(key, valid_parts))
			else:
				query_dict[key] = val

		self.query_dict = query_dict

		try:
			self.post(query_dict)
		except urllib2.HTTPError, e:
			raise IOError('Malformed query: \n {0}'.format(json.dumps(self.query_dict ,indent = 4)))
		else:
			self.result = self.parse()
			return self.parse()

	# implements a time boundary query
	def timeBoundary(self, **args):

		query_dict = {"queryType" : "timeBoundary"}
		valid_parts = ['dataSource']

		for key, val in args.iteritems():
			if key not in valid_parts:
				raise ValueError('{0} is not a valid query component. The list of valid components is: \n {1}'.format(key, valid_parts))
			else:
				query_dict[key] = val

		try:
			self.post(query_dict)
		except urllib2.HTTPError, e:
			raise IOError('Malformed query: \n {0}'.format(json.dumps(self.query_dict ,indent = 4)))
		else:
			self.result = self.parse()
			return self.parse()

	# prints internal variables of the object
	def describe(self):
		print("url: " + self.url)
