
import hashlib
import unittest
from time import sleep, time
from uuid import uuid4

from swift.metadata import sort_data
#from sort_dict4 import Sort_metadata


class Test_Sort_metadata(unittest.TestCase):

    def test_sort_data_helper(self):
        attr_list = [{"/AUTH_admin": {"account_name": "AUTH_admin","value1":"bb"}}, \
                     {"/AUTH_admin/testDir/dog.jpg": {"object_name": "dog.jpg","value1":"bb"}}, \
                     {"/AUTH_admin/testDir/cat.jpg": {"object_name": "cat.jpg","value1":"bba"}}]

        exp_result = [{'/AUTH_admin': {'account_name': 'AUTH_admin', 'value1': 'bb'}},\
                           {'/AUTH_admin/testDir/dog.jpg': {'object_name': 'dog.jpg', 'value1': 'bb'}},\
                           {'/AUTH_admin/testDir/cat.jpg': {'object_name': 'cat.jpg', 'value1': 'bba'}}]
        sort_value = "value1"
        Sotring = Sort_metadata()
        result =  Sotring.sort_data_helper (attr_list,sort_value)
        self.assertEquals(result, exp_result)

    def test_sort_data(self):
        attr_list = [{"/AUTH_admin": {"account_name": "AUTH_admin","value1":"bb"}}, \
                     {"/AUTH_admin/testDir/dog.jpg": {"object_name": "dog.jpg","value1":"bb"}}, \
                     {"/AUTH_admin/testDir/cat.jpg": {"object_name": "cat.jpg","value1":"bba"}}]


        exp_result = [{'/AUTH_admin': {'account_name': 'AUTH_admin', 'value1': 'bb'}}, \
                  {'/AUTH_admin/testDir/dog.jpg': {'object_name': 'dog.jpg', 'value1': 'bb'}}, \
                  {'/AUTH_admin/testDir/cat.jpg': {'object_name': 'cat.jpg', 'value1': 'bba'}}]
        sort_values = ["value1","uri"]
        sorting = Sort_metadata ()
        result = sorting.sort_data(attr_list,sort_values)
        self.assertEquals(result,exp_result)

if __name__ == '__main__':
    unittest.main()
