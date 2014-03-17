from collections import OrderedDict

#print len(r_list)

class Sort_metadata():
    def sort_data (self,attr_list,sort_values):
        dict1= {}
        dict2= {}
        dict3= {}
        return_dict = {}
        j=0
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




