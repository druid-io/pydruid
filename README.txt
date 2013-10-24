===========
pyDruid
===========

pyDruid provides a python interface to the Druid analytic store. Typical usage
often looks like this::

    #!/usr/bin/env python

    from pydruid.client import *

    # Druid Config
    endpoint = 'druid/v2/?pretty'
    demo_bard_url =  'http://localhost:8083'
    dataSource = 'webstream'
    intervals = ["2013-01-01/p1y"]

    query = pyDruid(demo_bard_url, endpoint)

    counts = query.timeseries(dataSource = dataSource, 
    	          granularity = "minute", 
    			  intervals = intervals, 
    			  aggregations = {"count" : doubleSum("rows")}
    		      )

    print counts
    [{'timestamp': '2013-09-30T23:31:00.000Z', 'result': {'count': 0.0}}, {'timestamp': '2013-09-30T23:32:00.000Z', 'result': {'count': 0.0}}, {'timestamp': '2013-09-30T23:33:00.000Z', 'result': {'count': 0.0}}, {'timestamp': '2013-09-30T23:34:00.000Z', 'result': {'count': 0.0}}]
