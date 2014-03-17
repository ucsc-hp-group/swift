from collections import OrderedDict

#print len(r_list)

class sort_metadata():
    def _init_(self,at_list,sr_value):
        #attr_list = [{"/AUTH_admin": {"account_name": "AUTH_admin"}}, {"/AUTH_admin/testDir/cat.jpg": {"object_name": "cat.jpg"}}, {"/AUTH_admin/testDir/dog.jpg": {"object_name": "dog.jpg"}}]
        #sort_values = "object_name"
        self.dict1= {}
        self.dict2= {}
        self.dict3= {}
        self.return_dict = {}
        self.j=0
        self.i=0

    def sort_values(self,attr_list,sort_values):
        for i in range(len(attr_list)):
            #print i
            dict1 = attr_list[i] #parsed list of dictionaries into a new dictionary (dict1) {uri : {user_meta_key : user_meta_value}}
            for d in dict1:
                dict2 = dict1[d] #Use a new dictionary to {user_meta_key : user_meta_value}
                for d in dict2:
                    if d == sort_values:    # Extract by only sort_value parameter (key)
                        dict3[dict2[d]]= j #store values from dictionaries as key in new dictionary to pass for sorting
                        j=j+1              #use incremental number value as placeholder for value in new dictionary (dict3)
        #print dict3
        for key in sorted(dict3):
            return_dict[sort_values]= key
            print return_dict
