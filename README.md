#pydruid
pydruid exposes a simple API to create, execute, and analyze [Druid](http://druid.io/) queries. pydruid can parse query results into [Pandas](http://pandas.pydata.org/) DataFrame objects for subsequent data analysis -- this offers a tight integration between [Druid](http://druid.io/), the [SciPy](http://www.scipy.org/stackspec.html) stack (for scientific computing) and [scikit-learn](http://scikit-learn.org/stable/) (for machine learning). Additionally, pydruid can export query results into TSV or JSON for further processing with your favorite tool, e.g., R, Julia, Matlab, Excel.
It provides both synchronous and asynchronous clients.

To install:
```python
pip install pydruid
# or, if you intend to use asynchronous client
pip install pydruid[async]
# or, if you intend to export query results into pandas
pip install pydruid[pandas]
# or, if you intend to do both
pip install pydruid[async, pandas]
```
Documentation: https://pythonhosted.org/pydruid/. 

#examples
The following exampes show how to execute and analyze the results of three types of queries: timeseries, topN, and groupby. We will use these queries to ask simple questions about twitter's public data set.

## timeseries 

What was the average tweet length, per day, surrounding the 2014 Sochi olympics?

```python
from pydruid.client import *
from pylab import plt

query = PyDruid(druid_url_goes_here, 'druid/v2')

ts = query.timeseries(
    datasource='twitterstream',
    granularity='day',
    intervals='2014-02-02/p4w',
    aggregations={'length': doublesum('tweet_length'), 'count': doublesum('count')},
    post_aggregations={'avg_tweet_length': (Field('length') / Field('count'))},
    filter=Dimension('first_hashtag') == 'sochi2014'
)
df = query.export_pandas()
df['timestamp'] = df['timestamp'].map(lambda x: x.split('T')[0])
df.plot(x='timestamp', y='avg_tweet_length', ylim=(80, 140), rot=20,
        title='Sochi 2014')
plt.ylabel('avg tweet length (chars)')
plt.show()
```

![alt text](https://github.com/metamx/pydruid/raw/master/docs/figures/avg_tweet_length.png "Avg. tweet length")

## topN 

Who were the top ten mentions (@user_name) during the 2014 Oscars?

```python
top = query.topn(
    datasource='twitterstream',
    granularity='all',
    intervals='2014-03-03/p1d',  # utc time of 2014 oscars
    aggregations={'count': doublesum('count')},
    dimension='user_mention_name',
    filter=(Dimension('user_lang') == 'en') & (Dimension('first_hashtag') == 'oscars') &
           (Dimension('user_time_zone') == 'Pacific Time (US & Canada)') &
           ~(Dimension('user_mention_name') == 'No Mention'),
    metric='count',
    threshold=10
)

df = query.export_pandas()
print df

   count                 timestamp user_mention_name
0   1303  2014-03-03T00:00:00.000Z      TheEllenShow
1     44  2014-03-03T00:00:00.000Z        TheAcademy
2     21  2014-03-03T00:00:00.000Z               MTV
3     21  2014-03-03T00:00:00.000Z         peoplemag
4     17  2014-03-03T00:00:00.000Z               THR
5     16  2014-03-03T00:00:00.000Z      ItsQueenElsa
6     16  2014-03-03T00:00:00.000Z           eonline
7     15  2014-03-03T00:00:00.000Z       PerezHilton
8     14  2014-03-03T00:00:00.000Z     realjohngreen
9     12  2014-03-03T00:00:00.000Z       KevinSpacey

```

## groupby

What does the social network of users replying to other users look like?

```python
from igraph import *
from cairo import *
from pandas import concat

group = query.groupby(
    datasource='twitterstream',
    granularity='hour',
    intervals='2013-10-04/pt12h',
    dimensions=["user_name", "reply_to_name"],
    filter=(~(Dimension("reply_to_name") == "Not A Reply")) &
           (Dimension("user_location") == "California"),
    aggregations={"count": doublesum("count")}
)

df = query.export_pandas()

# map names to categorical variables with a lookup table
names = concat([df['user_name'], df['reply_to_name']]).unique()
nameLookup = dict([pair[::-1] for pair in enumerate(names)])
df['user_name_lookup'] = df['user_name'].map(nameLookup.get)
df['reply_to_name_lookup'] = df['reply_to_name'].map(nameLookup.get)

# create the graph with igraph
g = Graph(len(names), directed=False)
vertices = zip(df['user_name_lookup'], df['reply_to_name_lookup'])
g.vs["name"] = names
g.add_edges(vertices)
layout = g.layout_fruchterman_reingold()
plot(g, "tweets.png", layout=layout, vertex_size=2, bbox=(400, 400), margin=25, edge_width=1, vertex_color="blue")
```

![alt text](https://github.com/metamx/pydruid/raw/master/docs/figures/twitter_graph.png "Social Network")

#asynchronous client
```pydruid.async_client.AsyncPyDruid``` implements an asynchronous client. To achieve that, it utilizes an asynchronous
HTTP client from ```Tornado``` framework. The asynchronous client is suitable for use with async frameworks such as Tornado
and provides much better performance at scale. It lets you serve multiple requests at the same time, without blocking on
Druid executing your queries.

##example
```python
from tornado import gen
from pydruid.async_client import AsyncPyDruid
from pydruid.utils.aggregators import longsum
from pydruid.utils.filters import Dimension

client = AsyncPyDruid(url_to_druid_broker, 'druid/v2')

@gen.coroutine
def your_asynchronous_method_serving_top10_mentions_for_day(day
    top_mentions = yield client.topn(
        datasource='twitterstream',
        granularity='all',
        intervals="%s/p1d" % (day, ),
        aggregations={'count': doublesum('count')},
        dimension='user_mention_name',
        filter=(Dimension('user_lang') == 'en') & (Dimension('first_hashtag') == 'oscars') &
               (Dimension('user_time_zone') == 'Pacific Time (US & Canada)') &
               ~(Dimension('user_mention_name') == 'No Mention'),
        metric='count',
        threshold=10)

    # asynchronously return results
    # can be simply ```return top_mentions``` in python 3.x
    raise gen.Return(top_mentions)
```
