import json

"""
[{'uri': {'obj_uri': 'val'}}, ...]


<object uri="/aaa/ccc/ooo">
    <object_name>ooo</object_name>
</object>
"""

def output_xml(metaList):
    """
    Converts the list of dicts into XML format
    """
    out = '<?xml version="1.0" encoding="UTF-8"?>\n\n'
    for d in metaList:
        uri = d.keys()[0]
        c = len(uri.split('/'))
        if c == 2:
            level = "account"
        elif c == 3:
            level = "container"
        elif c >= 4:
            level = "object"
            
        out += "<" + level + ' uri="' + uri + '">\n'
        
        for k in d[uri].keys():
            val = d[uri][k]
            out += "    <" + k + ">" + str(val) + "</" + k + ">\n"
        
        out += "</" + level + ">\n"
    return out
    
def output_plain(metaList):
    """
    Converts the list of dicts into a plain text format
    """
    out = ""
    for d in metaList:
        uri = d.keys()[0]
        out += uri + '\n'
        for k in d[uri].keys():
            val = d[uri][k]
            out += "    " + k + ":" + str(val) + '\n'
    return out 

def output_json(metaList):
    """
    Converts the list of dicts into a JSON format 
    """
    return json.dumps(metaList, indent=4, separators=(',', ' : '))
