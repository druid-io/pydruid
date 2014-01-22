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
from utils.aggregators import *
from utils.postaggregator import *
from utils.filters import *
from utils.query_utils import *

class pyDruid:

	def __init__(self,url,endpoint):
		self.url = url
		self.endpoint = endpoint
		self.result = None
		self.result_json = 	None
		self.query_type = None

	def post(self,query):
		try:
			querystr = json.dumps(query)
			if self.url.endswith('/'):
				url = self.url + self.endpoint
			else:
				url = self.url + '/' + self.endpoint
			headers = {'Content-Type' : 'application/json'}
			req = urllib2.Request(url, querystr, headers)
			res = urllib2.urlopen(req)
			data = res.read()
			self.result_json = data;
			res.close()			
		except urllib2.HTTPError, e:
			raise IOError('{0} \n Query is: {1}'.format(e, json.dumps(self.query_dict, indent = 4)))
		else:
			self.result = self.parse()
			parsed = self.parse()
			return parsed

	def parse(self):
		if self.result_json:
			res = json.loads(self.result_json)
			return res
		else:
			raise IOError('{Error parsing result: {0} for {1} query'.format(self.result_json, self.query_type))

	# --------- Export implementations --------- 

	def export_tsv(self,dest_path):
		f = open(dest_path,'wb')
		tsv_file = csv.writer(f, delimiter = '\t')

		if(self.query_type == "timeseries"):
			header = self.result[0]['result'].keys()
			header.append('timestamp')
		elif(self.query_type == "groupBy"):
			header = self.result[0]['event'].keys()
			header.append('timestamp')
			header.append('version')

		tsv_file.writerow(header)

		w = query_utils.UnicodeWriter(f)

		if self.result:
			if self.query_type == "topN" or self.query_type == "timeseries":
				for item in self.result:
					timestamp = item['timestamp']
					result = item['result']
					if type(result) is list: #topN
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

	def export_pandas(self): 
		if self.result:
			if self.query_type == "timeseries":
				nres = [v['result'].items() + [('timestamp',v['timestamp'])] for v in self.result]
				nres = [dict(v) for v in nres]
			elif self.query_type == "topN":
				nres = []
				for item in self.result:
					timestamp = item['timestamp']
					results = item['result']
					tres = [dict(res.items() + [('timestamp', timestamp)]) for res in results]
					nres += tres
			elif self.query_type == "groupBy":
				nres = [v['event'].items() + [('timestamp', v['timestamp'])] for v in self.result]
				nres = [dict(v) for v in nres]
			else:
				raise NotImplementedError('Pandas export not implemented for query type: {0}'.format(self.query_type))

			df = pandas.DataFrame(nres)
			return df	

	# --------- Query implementations --------- 
	def validateQuery(self, valid_parts, args):
		for key, val in args.iteritems():
			if key not in valid_parts:
				raise ValueError('{0} is not a valid query component. The list of valid components is: \n {1}'.format(key, valid_parts))

	def buildQuery(self, query_type, args):
		query_dict = {'queryType' : query_type}

		for key, val in args.iteritems():
			if key == "aggregations" :
				query_dict[key] = build_aggregators(val)
			elif key == "postAggregations" :
				query_dict[key] = build_post_aggregators(val)
			elif key == "filter" :
				query_dict[key] = build_filter(val)
			else:
				query_dict[key] = val

		self.query_dict = query_dict
		self.query_type = query_type

	def topN(self, **args):
		valid_parts = ['dataSource', 'granularity', 'filter', 'aggregations', 'postAggregations',
			'intervals', 'dimension', 'threshold', 'metric']
		self.validateQuery(valid_parts, args)
		self.buildQuery('topN', args)
		return self.post(self.query_dict)

	def timeseries(self, **args):
		valid_parts = ['dataSource', 'granularity', 'filter', 'aggregations', 'postAggregations',
			'intervals']
		self.validateQuery(valid_parts, args)
		self.buildQuery('timeseries', args)
		return self.post(self.query_dict)

	def groupBy(self, **args): 
		valid_parts = ['dataSource', 'granularity', 'filter', 'aggregations', 'postAggregations',
			'intervals', 'dimensions']
		self.validateQuery(valid_parts, args)
		self.buildQuery('groupBy', args)
		return self.post(self.query_dict)

	def segmentMetadata(self, **args): 
		valid_parts = ['dataSource', 'intervals']
		self.validateQuery(valid_parts, args)
		self.buildQuery('segmentMetaData', args)
		return self.post(self.query_dict)

	def timeBoundary(self, **args):
		valid_parts = ['dataSource']
		self.validateQuery(valid_parts, args)
		self.buildQuery('timeBoundary', args)
		return self.post(self.query_dict)
