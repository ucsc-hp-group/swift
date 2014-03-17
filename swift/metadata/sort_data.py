from collections import OrderedDict
import operator

# this calss will sort an attribute in a given list, and then return the sorted list 
# you can call the function like this
#attr_list = [{"/AUTH_admin": {"account_name": "AUTH_admin","value1":"bb"}}, {"/AUTH_admin/testDir/cat.jpg": {\
# "object_name": "cat.jpg","value1":"bab"}}, {"/AUTH_admin/testDir/dog.jpg": {"object_name": "dog.jpg","value1":"bba"}}]
#sort_values = "object_name"
# Sotring = Sort_metadata()
# Sotring.sort_data(attr_list,sort_values)



class Sort_metadata():
    def sort_data (self,attr_list,sort_value):
        dict1= {}
        dict2= {}
        dict3= {}
        index_list = []
        return_list = []
        #return_dict = {}
        j=0
        h=0
        for i in range(len(attr_list)):
            #print i
            dict1 = attr_list[i] #parsed list of dictionaries into a new dictionary (dict1) {uri : {user_meta_key : user_meta_value}}
            for d in dict1:
                dict2 = dict1[d] #Use a new dictionary to {user_meta_key : user_meta_value}
                for d in dict2:
                    if d == sort_value:    # Extract by only sort_value parameter (key)
                        dict3[j]= dict2[d] #store values from dictionaries as key in new dictionary to pass for sorting
                        j=j+1              #use incremental number value as placeholder for value in new dictionary (dict3)
        #print dict3

        sorted_dict = sorted(dict3.iteritems(), key=operator.itemgetter(1))
        print len(sorted_dict)

        for k in range(len(sorted_dict)):
            index_list.append(sorted_dict[k][0])
            return_list.append(attr_list[index_list[k]])

        for h in range(len(attr_list)):
            if not(h in index_list):
                return_list.append(attr_list[h])

        return return_list




