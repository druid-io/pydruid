import pprint as pp
import re

# determine whether arguments are strings or numeric constants
def is_numeric(field):
    for obj in [int, float, long]:
        try:
            obj(field)
            return True
        except:
            pass

    return False

# specify field type as source field name (string) or constant and build json
def field_spec(field):   
    if is_numeric(field):
        return {"type" : "constant",
                "name" : "constant_%d" % int(field),
                "value" : field}
    else:
        return {"type" : "fieldAccess", 
                "fieldName" : field}

# use regex to extract fields and operand from user-submitted postAgg string 
def parse_calc(postAggOpString):
    fields = []
    rgx = "(.+)[ ]?([*+/-])[ ]?(.+)"
    postAggOp = re.findall(rgx, postAggOpString).pop()
    
    if postAggOp:
        operation = postAggOp[0]
        fn = postAggOp[1]
        fields.append(field_spec(postAggOp[0]))
        fields.append(field_spec(postAggOp[2]))
        base = {"type" : "arithmetic",
                "fn" : fn,
                "fields" : fields}       
        return base
    else:
        raise Exception("Not a valid postAggregation operation")