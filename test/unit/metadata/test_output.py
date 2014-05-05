import hashlib
import unittest
from swift.metadata.output import *
from time import sleep, time
from uuid import uuid4
import sys
import os.path
# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))


"""
Each test fetches the fake meta data dictionaries (https://wiki.openstack.org/wiki/MetadataSearchAPI) to output functions.
Then it checks if output functions produce the same output as exp_result 
"""

class Test_Output_metadata(unittest.TestCase):
    
    def test_plain_text(self):
        attr_list = [{"/account1": {"account_container_count": "15"}},
                    {"/account1/container1": {"container_last_modified_time": "2013-07-23T13:17:55.435654031Z"}},
                    {"/account1/container1/objectdir1/subdir1/photo.jpg": {"object_last_changed_time": "2013-07-23T13:17:55.435654031Z","object_content_length":"194532"}},
                       {"/account1/container2": {"container_last_modified_time": "2013-07-23T13:17:55.435654031Z"}}
                   ]
    
        exp_result = "/account1" + '\n' + \
               "    account_container_count:15" + '\n' + \
               "/account1/container1" + '\n' + \
               "    container_last_modified_time:2013-07-23T13:17:55.435654031Z" + '\n' + \
               "/account1/container1/objectdir1/subdir1/photo.jpg" + '\n' + \
               "    object_content_length:194532" + '\n' + \
               "    object_last_changed_time:2013-07-23T13:17:55.435654031Z" + '\n' + \
               "/account1/container2" + '\n' + \
               "    container_last_modified_time:2013-07-23T13:17:55.435654031Z" + '\n'

        result = output_plain(attr_list)
        self.assertEquals(result, exp_result)
    
    def test_json(self):
        attr_list = [{"/account1": {"account_container_count": "15"}}]
      
        exp_result = "[" + '\n' + "    {" + '\n' + '        "/account1" : {' + '\n' + '            "account_container_count" : "15"' + '\n' + "        }" +'\n' + "    }" + '\n' + "]"     
    
        result = output_json(attr_list)
        self.assertEquals(result, exp_result)
    
    def test_xml(self):
        attr_list = [{"/account1": {"account_container_count": "15"}}]
     
        exp_result = '<?xml version="1.0" encoding="UTF-8"?>' + '\n' + '\n' + \
                     '<account uri="/account1">' + '\n' + \
                     '    <account_container_count>15</account_container_count>' + '\n' + \
                     '</account>' + '\n'
        result = output_xml(attr_list)
        self.assertEquals(result, exp_result)
    
    """	
    It assumes that format is plain/text
    """
    
    def test_no_format(self):
        attr_list = [{"/account1": {"account_container_count": "15"}},
                    {"/account1/container1": {"container_last_modified_time": "2013-07-23T13:17:55.435654031Z"}},
                    {"/account1/container1/objectdir1/subdir1/photo.jpg": {"object_last_changed_time": "2013-07-23T13:17:55.435654031Z","object_content_length":"194532"}},
                       {"/account1/container2": {"container_last_modified_time": "2013-07-23T13:17:55.435654031Z"}}
                       ]    
    
        exp_result = "/account1" + '\n' + \
               "    account_container_count:15" + '\n' + \
               "/account1/container1" + '\n' + \
               "    container_last_modified_time:2013-07-23T13:17:55.435654031Z" + '\n' + \
               "/account1/container1/objectdir1/subdir1/photo.jpg" + '\n' + \
               "    object_content_length:194532" + '\n' + \
               "    object_last_changed_time:2013-07-23T13:17:55.435654031Z" + '\n' + \
               "/account1/container2" + '\n' + \
               "    container_last_modified_time:2013-07-23T13:17:55.435654031Z" + '\n'
               
        result = output_plain(attr_list)
        self.assertEquals(result, exp_result)
    
    """
    It assumes that format is plain/text
    """
    
    def test_invalid_format(self):
        attr_list = [{"/account1": {"account_container_count": "15"}},
                       {"/account1/container1": {"container_last_modified_time": "2013-07-23T13:17:55.435654031Z"}},
                       {"/account1/container1/objectdir1/subdir1/photo.jpg": {"object_last_changed_time": "2013-07-23T13:17:55.435654031Z","object_content_length":"194532"}},
                {"/account1/container2": {"container_last_modified_time": "2013-07-23T13:17:55.435654031Z"}}
                ]
    
        exp_result = "/account1" + '\n' + \
               "    account_container_count:15" + '\n' + \
               "/account1/container1" + '\n' + \
               "    container_last_modified_time:2013-07-23T13:17:55.435654031Z" + '\n' + \
               "/account1/container1/objectdir1/subdir1/photo.jpg" + '\n' + \
               "    object_content_length:194532" + '\n' + \
               "    object_last_changed_time:2013-07-23T13:17:55.435654031Z" + '\n' + \
               "/account1/container2" + '\n' + \
               "    container_last_modified_time:2013-07-23T13:17:55.435654031Z" + '\n'
       
        result = output_plain(attr_list)
        self.assertEquals(result, exp_result)
    
if __name__ == '__main__':
    unittest.main()
