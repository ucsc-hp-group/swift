
import hashlib
import unittest
from time import sleep, time
from uuid import uuid4

from swift.metadata.sort_data import Sort_metadata
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

if __name__ == '__main__':
    unittest.main()