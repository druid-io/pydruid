#create a json list of user-defined aggregations

# Define the support aggregation types

def longSum(raw_metric):
	return {"type" : "longSum", "fieldName" : raw_metric}

def doubleSum(raw_metric):
	return {"type" : "doubleSum", "fieldName" : raw_metric}

def min(raw_metric):
	return {"type" : "min", "fieldName" : raw_metric}

def max(raw_metric):
	return {"type" : "max", "fieldName" : raw_metric}

def count(raw_metric):
	return {"type" : "count", "fieldName" : raw_metric}

def build_aggregators(agg_input):
	return [dict([('name',k)] + v.items()) for (k,v) in agg_input.iteritems()]