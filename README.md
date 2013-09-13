===========
pyDruid
===========

pyDruid provides a python interface to the Druid analytic store. Typical usage
often looks like this::

    #!/usr/bin/env python

    from pyDruid import *

    # Druid Config
    endpoint = 'druid/v2/?pretty'
    demo_bard_url =  'http://localhost:8083'
    dataSource = 'wikipedia'
    intervals = ["2013-01-01/p1y"]

    query = pyDruid(demo_bard_url, endpoint)

    counts = query.timeseries(dataSource = dataSource, 
    	          granularity = "minute", 
				  intervals = intervals, 
				  aggregations = {"count" : doubleSum("edits")}
			      )
