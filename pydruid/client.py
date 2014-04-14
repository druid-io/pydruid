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

try:
    import pandas
except ImportError:
    print 'Warning: unable to import Pandas. The export_pandas method will not work.'
    pass

from utils.aggregators import *
from utils.postaggregator import *
from utils.filters import *
from utils.query_utils import *


class PyDruid:
    """
    PyDruid contains the functions for creating and executing Druid queries, as well as
    for exporting query results into TSV files or pandas.DataFrame objects for subsequent analysis.

    :param str url: URL of Bard node in the Druid cluster
    :param str endpoint: Endpoint that Bard listens for queries on

    :ivar str result_json: JSON object representing a query result. Initial value: None
    :ivar list result: Query result parsed into a list of dicts. Initial value: None
    :ivar str query_type: Name of most recently run query, e.g., topN. Initial value: None
    :ivar dict query_dict: JSON object representing the query. Initial value: None

    Example

    .. code-block:: python
        :linenos:

            >>> from pydruid.client import *

            >>> query = PyDruid('http://localhost:8083', 'druid/v2/')

            >>> top = query.topn(
                    datasource='twitterstream',
                    granularity='all',
                    intervals='2013-10-04/pt1h',
                    aggregations={"count": doublesum("count")},
                    dimension='user_name',
                    filter = Dimension('user_lang') == 'en',
                    metric='count',
                    threshold=2
                )

            >>> print json.dumps(query.query_dict, indent=2)
            >>> {
                  "metric": "count",
                  "aggregations": [
                    {
                      "type": "doubleSum",
                      "fieldName": "count",
                      "name": "count"
                    }
                  ],
                  "dimension": "user_name",
                  "filter": {
                    "type": "selector",
                    "dimension": "user_lang",
                    "value": "en"
                  },
                  "intervals": "2013-10-04/pt1h",
                  "dataSource": "twitterstream",
                  "granularity": "all",
                  "threshold": 2,
                  "queryType": "topN"
                }

            >>> print query.result
            >>> [{'timestamp': '2013-10-04T00:00:00.000Z',
                'result': [{'count': 7.0, 'user_name': 'user_1'}, {'count': 6.0, 'user_name': 'user_2'}]}]

            >>> df = query.export_pandas()
            >>> print df
            >>>    count                 timestamp      user_name
                0      7  2013-10-04T00:00:00.000Z         user_1
                1      6  2013-10-04T00:00:00.000Z         user_2
    """

    def __init__(self, url, endpoint):
        self.url = url
        self.endpoint = endpoint
        self.result = None
        self.result_json = None
        self.query_type = None
        self.query_dict = None

    def __post(self, query):
        try:
            querystr = json.dumps(query)
            if self.url.endswith('/'):
                url = self.url + self.endpoint
            else:
                url = self.url + '/' + self.endpoint
            headers = {'Content-Type': 'application/json'}
            req = urllib2.Request(url, querystr, headers)
            res = urllib2.urlopen(req)
            data = res.read()
            self.result_json = data
            res.close()
        except urllib2.HTTPError, e:
            raise IOError('{0} \n Query is: {1}'.format(
                e, json.dumps(self.query_dict, indent=4)))
        else:
            self.result = self.__parse()
            return self.result

    def __parse(self):
        if self.result_json:
            res = json.loads(self.result_json)
            return res
        else:
            raise IOError('{Error parsing result: {0} for {1} query'.format(
                self.result_json, self.query_type))

    # --------- Export implementations ---------

    def export_tsv(self, dest_path):
        """
        Export the current query result to a tsv file.

        :param str dest_path: file to write query results to
        :raise NotImplementedError:

        Example

        .. code-block:: python
            :linenos:

                >>> top = query.topn(
                        datasource='twitterstream',
                        granularity='all',
                        intervals='2013-10-04/pt1h',
                        aggregations={"count": doublesum("count")},
                        dimension='user_name',
                        filter = Dimension('user_lang') == 'en',
                        metric='count',
                        threshold=2
                    )

                >>> query.export_tsv('top.tsv')
                >>> !cat top.tsv
                >>> count	user_name	timestamp
                    7.0	user_1	2013-10-04T00:00:00.000Z
                    6.0	user_2	2013-10-04T00:00:00.000Z
        """
        f = open(dest_path, 'wb')
        tsv_file = csv.writer(f, delimiter='\t')

        if self.query_type == "timeseries":
            header = self.result[0]['result'].keys()
            header.append('timestamp')
        if self.query_type == 'topN':
            header = self.result[0]['result'][0].keys()
            header.append('timestamp')
        elif self.query_type == "groupBy":
            header = self.result[0]['event'].keys()
            header.append('timestamp')
            header.append('version')
        else:
            raise NotImplementedError('TSV export not implemented for query type: {0}'.format(self.query_type))

        tsv_file.writerow(header)

        w = UnicodeWriter(f)

        if self.result:
            if self.query_type == "topN" or self.query_type == "timeseries":
                for item in self.result:
                    timestamp = item['timestamp']
                    result = item['result']
                    if type(result) is list:  # topN
                        for line in result:
                            w.writerow(line.values() + [timestamp])
                    else:  # timeseries
                        w.writerow(result.values() + [timestamp])
            elif self.query_type == "groupBy":
                for item in self.result:
                    timestamp = item['timestamp']
                    version = item['version']
                    w.writerow(
                        item['event'].values() + [timestamp] + [version])

        f.close()

    def export_pandas(self):
        """
        Export the current query result to a Pandas DataFrame object.

        :return: The DataFrame representing the query result
        :rtype: DataFrame
        :raise NotImplementedError:

        Example

        .. code-block:: python
            :linenos:

                >>> top = query.topn(
                        datasource='twitterstream',
                        granularity='all',
                        intervals='2013-10-04/pt1h',
                        aggregations={"count": doublesum("count")},
                        dimension='user_name',
                        filter = Dimension('user_lang') == 'en',
                        metric='count',
                        threshold=2
                    )

                >>> df = query.export_pandas()
                >>> print df
                >>>    count                 timestamp      user_name
                    0      7  2013-10-04T00:00:00.000Z         user_1
                    1      6  2013-10-04T00:00:00.000Z         user_2
        """
        if self.result:
            if self.query_type == "timeseries":
                nres = [v['result'].items() + [('timestamp', v['timestamp'])]
                        for v in self.result]
                nres = [dict(v) for v in nres]
            elif self.query_type == "topN":
                nres = []
                for item in self.result:
                    timestamp = item['timestamp']
                    results = item['result']
                    tres = [dict(res.items() + [('timestamp', timestamp)])
                            for res in results]
                    nres += tres
            elif self.query_type == "groupBy":
                nres = [v['event'].items() + [('timestamp', v['timestamp'])]
                        for v in self.result]
                nres = [dict(v) for v in nres]
            else:
                raise NotImplementedError('Pandas export not implemented for query type: {0}'.format(self.query_type))

            df = pandas.DataFrame(nres)
            return df

    # --------- Query implementations ---------

    def validate_query(self, valid_parts, args):
        for key, val in args.iteritems():
            if key not in valid_parts:
                raise ValueError(
                    'Query component: {0} is not valid for query type: {1}.'
                    .format(key, self.query_type) +
                    'The list of valid components is: \n {0}'
                    .format(valid_parts))

    def build_query(self, args):
        query_dict = {'queryType': self.query_type}

        for key, val in args.iteritems():
            if key == 'aggregations':
                query_dict[key] = build_aggregators(val)
            elif key == 'post_aggregations':
                query_dict['postAggregations'] = Postaggregator.build_post_aggregators(val)
            elif key == 'datasource':
                query_dict['dataSource'] = val
            elif key == "filter":
                query_dict[key] = Filter.build_filter(val)
            else:
                query_dict[key] = val

        self.query_dict = query_dict

    def topn(self, **kwargs):
        """
        A TopN query returns a set of the values in a given dimension, sorted by a specified metric. Conceptually, a
        topN can be thought of as an approximate GroupByQuery over a single dimension with an Ordering spec. TopNs are
        faster and more resource efficient than GroupBy for this use case.

        Required key/value pairs:

        :param str datasource: Data source to query
        :param str granularity: Aggregate data by hour, day, minute, etc.,
        :param intervals: ISO-8601 intervals of data to query
        :type intervals: str or list
        :param dict aggregations: A map from aggregator name to one of the pydruid.utils.aggregators e.g., doublesum
        :param str dimension: Dimension to run the query against
        :param str metric: Metric over which to sort the specified dimension by
        :param int threshold: How many of the top items to return

        :return: The query result
        :rtype: list[dict]

        Optional key/value pairs:

        :param pydruid.utils.filters.Filter filter: Indicates which rows of data to include in the query
        :param post_aggregations:   A dict with string key = 'post_aggregator_name', and value pydruid.utils.PostAggregator

        Example:

        .. code-block:: python
            :linenos:

                >>> top = query.topn(
                            datasource='twitterstream',
                            granularity='all',
                            intervals='2013-06-14/pt1h',
                            aggregations={"count": doublesum("count")},
                            dimension='user_name',
                            metric='count',
                            filter=Dimension('user_lang') == 'en',
                            threshold=1
                        )
                >>> print top
                >>> [{'timestamp': '2013-06-14T00:00:00.000Z', 'result': [{'count': 22.0, 'user': "cool_user"}}]}]
        """
        self.query_type = 'topN'
        valid_parts = [
            'datasource', 'granularity', 'filter', 'aggregations',
            'post_aggregations', 'intervals', 'dimension', 'threshold',
            'metric'
        ]
        self.validate_query(valid_parts, kwargs)
        self.build_query(kwargs)
        return self.__post(self.query_dict)

    def timeseries(self, **kwargs):
        """
        A timeseries query returns the values of the requested metrics (in aggregate) for each timestamp.

        Required key/value pairs:

        :param str datasource: Data source to query
        :param str granularity: Time bucket to aggregate data by hour, day, minute, etc.,
        :param intervals: ISO-8601 intervals for which to run the query on
        :type intervals: str or list
        :param dict aggregations: A map from aggregator name to one of the pydruid.utils.aggregators e.g., doublesum

        :return: The query result
        :rtype: list[dict]

        Optional key/value pairs:

        :param pydruid.utils.filters.Filter filter: Indicates which rows of data to include in the query
        :param post_aggregations:   A dict with string key = 'post_aggregator_name', and value pydruid.utils.PostAggregator

        Example:

        .. code-block:: python
            :linenos:

                >>> counts = query.timeseries(
                        datasource=twitterstream,
                        granularity='hour',
                        intervals='2013-06-14/pt1h',
                        aggregations={"count": doublesum("count"), "rows": count("rows")},
                        post_aggregations={'percent': (Field('count') / Field('rows')) * Const(100))}
                    )
                >>> print counts
                >>> [{'timestamp': '2013-06-14T00:00:00.000Z', 'result': {'count': 9619.0, 'rows': 8007, 'percent': 120.13238416385663}}]
        """
        self.query_type = 'timeseries'
        valid_parts = [
            'datasource', 'granularity', 'filter', 'aggregations',
            'post_aggregations', 'intervals'
        ]
        self.validate_query(valid_parts, kwargs)
        self.build_query(kwargs)
        return self.__post(self.query_dict)

    def groupby(self, **kwargs):
        """
        A group-by query groups a results set (the requested aggregate metrics) by the specified dimension(s).

        Required key/value pairs:

        :param str datasource: Data source to query
        :param str granularity: Time bucket to aggregate data by hour, day, minute, etc.,
        :param intervals: ISO-8601 intervals for which to run the query on
        :type intervals: str or list
        :param dict aggregations: A map from aggregator name to one of the pydruid.utils.aggregators e.g., doublesum
        :param list dimensions: The dimensions to group by

        :return: The query result
        :rtype: list[dict]

        Optional key/value pairs:

        :param pydruid.utils.filters.Filter filter: Indicates which rows of data to include in the query
        :param post_aggregations:   A dict with string key = 'post_aggregator_name', and value pydruid.utils.PostAggregator

        Example:

        .. code-block:: python
            :linenos:

                >>> group = query.groupby(
                        dataSource='twitterstream',
                        granularity='hour',
                        intervals='2013-10-04/pt1h',
                        dimensions=["user_name", "reply_to_name"],
                        filter=~(Dimension("reply_to_name") == "Not A Reply"),
                        aggregations={"count": doublesum("count")}
                    )
                >>> for k in range(2):
                    ...     print group[k]
                >>> {'timestamp': '2013-10-04T00:00:00.000Z', 'version': 'v1', 'event': {'count': 1.0, 'user_name': 'user_1', 'reply_to_name': 'user_2'}}
                >>> {'timestamp': '2013-10-04T00:00:00.000Z', 'version': 'v1', 'event': {'count': 1.0, 'user_name': 'user_2', 'reply_to_name': 'user_3'}}
        """

        self.query_type = 'groupBy'
        valid_parts = [
            'datasource', 'granularity', 'filter', 'aggregations',
            'post_aggregations', 'intervals', 'dimensions'
        ]
        self.validate_query(valid_parts, kwargs)
        self.build_query(kwargs)
        return self.__post(self.query_dict)

    def segment_metadata(self, **kwargs):
        """
        A segment meta-data query returns per segment information about:

        * Cardinality of all the columns present
        * Column type
        * Estimated size in bytes
        * Estimated size in bytes of each column
        * Interval the segment covers
        * Segment ID

        Required key/value pairs:

        :param str datasource: Data source to query
        :param intervals: ISO-8601 intervals for which to run the query on
        :type intervals: str or list

        :return: The query result
        :rtype: list[dict]

        Example:

        .. code-block:: python
            :linenos:

                >>> meta = query.segment_metadata(datasource='twitterstream', intervals = '2013-10-04/pt1h')
                >>> print meta[0].keys()
                >>> ['intervals', 'id', 'columns', 'size']
                >>> print meta[0]['columns']['tweet_length']
                >>> {'errorMessage': None, 'cardinality': None, 'type': 'FLOAT', 'size': 30908008}

        """
        self.query_type = 'segmentMetadata'
        valid_parts = ['datasource', 'intervals']
        self.validate_query(valid_parts, kwargs)
        self.build_query(kwargs)
        return self.__post(self.query_dict)

    def time_boundary(self, **kwargs):
        """
        A time boundary query returns the min and max timestamps present in a data source.

        Required key/value pairs:

        :param str datasource: Data source to query

        :return: The query result
        :rtype: list[dict]

        Example:

        .. code-block:: python
            :linenos:

                >>> bound = query.time_boundary(datasource='twitterstream')
                >>> print bound
                >>> [{'timestamp': '2011-09-14T15:00:00.000Z', 'result': {'minTime': '2011-09-14T15:00:00.000Z', 'maxTime': '2014-03-04T23:44:00.000Z'}}]
        """
        self.query_type = 'timeBoundary'
        valid_parts = ['datasource']
        self.validate_query(valid_parts, kwargs)
        self.build_query(kwargs)
        return self.__post(self.query_dict)
