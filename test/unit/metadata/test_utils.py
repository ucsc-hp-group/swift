# Copyright (c) 2010-2012 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from swift.metadata.utils import *
#from sort_dict4 import Sort_metadata


class Test_Sort_metadata(unittest.TestCase):

    def test_sort_data_helper(self):
        attr_list = [{"/AUTH_admin/testDir/dog.jpg": {"object_name": "dog.jpg","value1":"ccc"}},
                    {"/AUTH_admin": {"account_name": "AUTH_admin","value1":"aaa"}},
                     {"/AUTH_admin/testDir/cat.jpg": {"object_name": "cat.jpg","value1":"bbb"}}]

        exp_result = [{'/AUTH_admin': {'account_name': 'AUTH_admin', 'value1': 'aaa'}},
                           {'/AUTH_admin/testDir/cat.jpg': {'object_name': 'cat.jpg', 'value1': 'bbb'}},
                           {'/AUTH_admin/testDir/dog.jpg': {'object_name': 'dog.jpg', 'value1': 'ccc'}}]
        sort_value = "value1"
        sorting = Sort_metadata()
        result = sorting.sort_data_helper(attr_list,sort_value)
        self.assertEquals(result, exp_result)

    def test_sort_data(self):
        attr_list = [{"/AUTH_admin/testDir/dog.jpg": {"object_name": "dog.jpg","value1":"bbb"}},
                    {"/AUTH_admin": {"account_name": "AUTH_admin","value1":"aaa"}},
                     {"/AUTH_admin/testDir/cat.jpg": {"object_name": "cat.jpg","value1":"bbb"}}]

        exp_result = [{'/AUTH_admin': {'account_name': 'AUTH_admin', 'value1': 'aaa'}},
                           {'/AUTH_admin/testDir/cat.jpg': {'object_name': 'cat.jpg', 'value1': 'bbb'}},
                           {'/AUTH_admin/testDir/dog.jpg': {'object_name': 'dog.jpg', 'value1': 'bbb'}}]
        sort_values = ["value1","uri"]
        sorting = Sort_metadata ()
        result = sorting.sort_data(attr_list,sort_values)
        self.assertEquals(result,exp_result)

    def test_multiple_attr_types_sorted_by_uri(self):
        attr_list = [{"/AUTH_admin/testDir/dog.jpg": {"object_name": "dog.jpg","value1":"ccc"}},
                    {"/AUTH_admin": {"account_name": "AUTH_admin","value1":"aaa"}},
                     {"/AUTH_admin/testDir/cat.jpg": {"object_name": "cat.jpg","value1":"bbb"}}]

        exp_result = [{'/AUTH_admin': {'account_name': 'AUTH_admin', 'value1': 'aaa'}},
                           {'/AUTH_admin/testDir/cat.jpg': {'object_name': 'cat.jpg', 'value1': 'bbb'}},
                           {'/AUTH_admin/testDir/dog.jpg': {'object_name': 'dog.jpg', 'value1': 'ccc'}}]
        sort_values = ['uri']
        sorting = Sort_metadata()
        result = sorting.sort_data(attr_list,sort_values)
        self.assertEquals(result,exp_result)


"""
Each test fetches the fake meta data dictionaries (https://wiki.openstack.org/wiki/MetadataSearchAPI) to output functions.
Then it checks if output functions produce the same output as exp_result 
"""

class Test_Output_metadata(unittest.TestCase):
    def test_plain_text(self):
        attr_list = [{"/account1": {"account_container_count": "15"}},
                    {"/account1/container1": {"container_last_modified_time": "2013-07-23T13:17:55.435654031Z"}},
                    {"/account1/container1/objectdir1/subdir1/photo.jpg": {"object_last_changed_time": "2013-07-23T13:17:55.435654031Z","object_content_length":"194532"}},
                    {"/account1/container2": {"container_last_modified_time": "2013-07-23T13:17:55.435654031Z"}},
                    {"/account1/container2/anotherObject": {"object_last_changed_time": "2012-12-02T00:53:29.849922518Z","object_content_length": "194532"}}
                    ]
    
        exp_result = "/account1" + '\n' + \
                     "    account_container_count:15" + '\n' + \
                     "/account1/container1" + '\n' + \
                     "    container_last_modified_time:2013-07-23T13:17:55.435654031Z" + '\n' + \
                     "/account1/container1/objectdir1/subdir1/photo.jpg" + '\n' + \
                     "    object_content_length:194532" + '\n' + \
                     "    object_last_changed_time:2013-07-23T13:17:55.435654031Z" + '\n' + \
                     "/account1/container2" + '\n' + \
                     "    container_last_modified_time:2013-07-23T13:17:55.435654031Z" + '\n' +\
                     "/account1/container2/anotherObject" + '\n' + \
                     "    object_content_length:194532" + '\n' + \
                     "    object_last_changed_time:2012-12-02T00:53:29.849922518Z" + '\n'

        result = output_plain(attr_list)
        self.assertEquals(result, exp_result)
    
    def test_json(self):
        attr_list = [{"/account1": {"account_container_count": "15"}},
                    {"/account1/container1": {"container_last_modified_time": "2013-07-23T13:17:55.435654031Z"}},
                    {"/account1/container1/objectdir1/subdir1/photo.jpg": {"object_last_changed_time": "2013-07-23T13:17:55.435654031Z","object_content_length":"194532"}},
                    {"/account1/container2": {"container_last_modified_time": "2013-07-23T13:17:55.435654031Z"}},
                    {"/account1/container2/anotherObject": {"object_last_changed_time": "2012-12-02T00:53:29.849922518Z","object_content_length": "194532"}}
                    ]
      
        exp_result = '[' + '\n' + \
                     '    {' + '\n' + \
                     '        "/account1" : {' + '\n' + \
                     '            "account_container_count" : "15"' + '\n' + \
                     '        }' +'\n' + \
                     '    },' + '\n' + \
                     '    {' + '\n' + \
                     '        "/account1/container1" : {' + '\n' + \
                     '            "container_last_modified_time" : "2013-07-23T13:17:55.435654031Z"' + '\n' + \
                     '        }' +'\n' + \
                     '    },' + '\n' + \
                     '    {' + '\n' + \
                     '        "/account1/container1/objectdir1/subdir1/photo.jpg" : {' + '\n' + \
                     '            "object_content_length" : "194532",' + '\n' + \
                     '            "object_last_changed_time" : "2013-07-23T13:17:55.435654031Z"' + '\n' + \
                     '        }' +'\n' + \
                     '    },' + '\n' + \
                     '    {' + '\n' + \
                     '        "/account1/container2" : {' + '\n' + \
                     '            "container_last_modified_time" : "2013-07-23T13:17:55.435654031Z"' + '\n' + \
                     '        }' +'\n' + \
                     '    },' + '\n' + \
                     '    {' + '\n' + \
                     '        "/account1/container2/anotherObject" : {' + '\n' + \
                     '            "object_content_length" : "194532",' + '\n' + \
                     '            "object_last_changed_time" : "2012-12-02T00:53:29.849922518Z"' + '\n' + \
                     '        }' +'\n' + \
                     '    }' + '\n' + \
                     ']'
    
        result = output_json(attr_list)
        self.assertEquals(result, exp_result)
    
    def test_xml(self):
        attr_list = [{"/account1": {"account_container_count": "15"}},
                    {"/account1/container1": {"container_last_modified_time": "2013-07-23T13:17:55.435654031Z"}},
                    {"/account1/container1/objectdir1/subdir1/photo.jpg": {"object_last_changed_time": "2013-07-23T13:17:55.435654031Z","object_content_length":"194532"}},
                    {"/account1/container2": {"container_last_modified_time": "2013-07-23T13:17:55.435654031Z"}},
                    {"/account1/container2/anotherObject": {"object_last_changed_time": "2012-12-02T00:53:29.849922518Z","object_content_length": "194532"}}
                    ]
     
        exp_result = '<?xml version="1.0" encoding="UTF-8"?>' + '\n' + '\n' + \
                     '<metadata>' + '\n' + \
                     '<account uri="/account1">' + '\n' + \
                     '    <account_container_count>15</account_container_count>' + '\n' + \
                     '</account>' + '\n' + \
                     '<container uri="/account1/container1">' +'\n' + \
                     '    <container_last_modified_time>2013-07-23T13:17:55.435654031Z</container_last_modified_time>' +'\n' + \
                     '</container>' +'\n' + \
                     '<object uri="/account1/container1/objectdir1/subdir1/photo.jpg">' +'\n' + \
                     '    <object_content_length>194532</object_content_length>' +'\n' + \
                     '    <object_last_changed_time>2013-07-23T13:17:55.435654031Z</object_last_changed_time>' +'\n' + \
                     '</object>' +'\n' + \
                     '<container uri="/account1/container2">' +'\n' + \
                     '    <container_last_modified_time>2013-07-23T13:17:55.435654031Z</container_last_modified_time>' +'\n' + \
                     '</container>' +'\n' + \
                     '<object uri="/account1/container2/anotherObject">' +'\n' + \
                     '    <object_content_length>194532</object_content_length>' +'\n' + \
                     '    <object_last_changed_time>2012-12-02T00:53:29.849922518Z</object_last_changed_time>' +'\n' + \
                     '</object>' +'\n' + \
                     '</metadata>' + '\n'

        result = output_xml(attr_list)
        self.assertEquals(result, exp_result)
    
    """	
    It assumes that format is plain/text
    """
    
    def test_no_format(self):
        attr_list = [{"/account1": {"account_container_count": "15"}},
                    {"/account1/container1": {"container_last_modified_time": "2013-07-23T13:17:55.435654031Z"}},
                    {"/account1/container1/objectdir1/subdir1/photo.jpg": {"object_last_changed_time": "2013-07-23T13:17:55.435654031Z","object_content_length":"194532"}},
                    {"/account1/container2": {"container_last_modified_time": "2013-07-23T13:17:55.435654031Z"}},
                    {"/account1/container2/anotherObject": {"object_last_changed_time": "2012-12-02T00:53:29.849922518Z","object_content_length": "194532"}}
                    ]
    
        exp_result = "/account1" + '\n' + \
                     "    account_container_count:15" + '\n' + \
                     "/account1/container1" + '\n' + \
                     "    container_last_modified_time:2013-07-23T13:17:55.435654031Z" + '\n' + \
                     "/account1/container1/objectdir1/subdir1/photo.jpg" + '\n' + \
                     "    object_content_length:194532" + '\n' + \
                     "    object_last_changed_time:2013-07-23T13:17:55.435654031Z" + '\n' + \
                     "/account1/container2" + '\n' + \
                     "    container_last_modified_time:2013-07-23T13:17:55.435654031Z" + '\n' +\
                     "/account1/container2/anotherObject" + '\n' + \
                     "    object_content_length:194532" + '\n' + \
                     "    object_last_changed_time:2012-12-02T00:53:29.849922518Z" + '\n'
               
        result = output_plain(attr_list)
        self.assertEquals(result, exp_result)
    
    """
    It assumes that format is plain/text
    """
    
    def test_invalid_format(self):
        attr_list = [{"/account1": {"account_container_count": "15"}},
                    {"/account1/container1": {"container_last_modified_time": "2013-07-23T13:17:55.435654031Z"}},
                    {"/account1/container1/objectdir1/subdir1/photo.jpg": {"object_last_changed_time": "2013-07-23T13:17:55.435654031Z","object_content_length":"194532"}},
                    {"/account1/container2": {"container_last_modified_time": "2013-07-23T13:17:55.435654031Z"}},
                    {"/account1/container2/anotherObject": {"object_last_changed_time": "2012-12-02T00:53:29.849922518Z","object_content_length": "194532"}}
                    ]
    
        exp_result = "/account1" + '\n' + \
                     "    account_container_count:15" + '\n' + \
                     "/account1/container1" + '\n' + \
                     "    container_last_modified_time:2013-07-23T13:17:55.435654031Z" + '\n' + \
                     "/account1/container1/objectdir1/subdir1/photo.jpg" + '\n' + \
                     "    object_content_length:194532" + '\n' + \
                     "    object_last_changed_time:2013-07-23T13:17:55.435654031Z" + '\n' + \
                     "/account1/container2" + '\n' + \
                     "    container_last_modified_time:2013-07-23T13:17:55.435654031Z" + '\n' +\
                     "/account1/container2/anotherObject" + '\n' + \
                     "    object_content_length:194532" + '\n' + \
                     "    object_last_changed_time:2012-12-02T00:53:29.849922518Z" + '\n'
       
        result = output_plain(attr_list)
        self.assertEquals(result, exp_result)
    
if __name__ == '__main__':
    unittest.main()