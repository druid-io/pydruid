#pydruid
pydruid exposes a simple API to create, execute, and analyze [Druid](http://druid.io/) queries. pydruid can parse query results into [Pandas](http://pandas.pydata.org/) DataFrame objects for subsequent data analysis -- this offers a tight integration between [Druid](http://druid.io/), the [SciPy](http://www.scipy.org/stackspec.html) stack (for scientific computing) and [scikit-learn](http://scikit-learn.org/stable/) (for machine learning). Additionally, pydruid can export query results into TSV or JSON for further processing with your favorite tool, e.g., R, Julia, Matlab, Excel.

#setup

#documentation

#examples

The following exampes show how to execute and analyze the results of three types of queries:timeseries, topN, and groupby. We analyze the twitter data set

## timeseries query

What was the average tweet length, per day, surrounding the 2014 Sochi olympics?

```python
from pydruid.client import *
from pylab import plt

query = PyDruid(bard_url_goes_here, 'druid/v2')

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

![alt text](https://github.com/metamx/pydruid/raw/docs/docs/figures/avg_tweet_length.png "Avg. tweet length")






