from swift.common.utils import json

"""
[{'uri': {'obj_uri': 'val'}}, ...]


<object uri="/aaa/ccc/ooo">
    <object_name>ooo</object_name>
</object>
"""

"""
dic = [{"/account1": {"account_container_count": "15"}},
      {"/account1/container1": {"container_last_modified_time": "2013-07-23T13:17:55.435654031Z"}},
      {"/account1/container1/objectdir1/subdir1/photo.jpg": {"object_last_changed_time": "2013-07-23T13:17:55.435654031Z","object_content_length":"194532"}},
      {"/account1/container2": {"container_last_modified_time": "2013-07-23T13:17:55.435654031Z"}},
      {"/account1/container2/anotherObject": {"object_last_changed_time": "2012-12-02T00:53:29.849922518Z","object_content_length": "194532"}}]
"""


def output_xml(metaList):
    """
    Converts the list of dicts into XML format
    """
    out = '<?xml version="1.0" encoding="UTF-8"?>\n\n'
    out += "<metadata>" +'\n'

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
    out += "</metadata>" + '\n'
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

"""
print output_json(dic)
print output_plain(dic)
print output_xml(dic)
"""
