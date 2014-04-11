from collections import OrderedDict
import operator


class Sort_metadata():
#sort the dic by one attr
    def sort_data_helper (self, attr_list, sort_value):
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
                        dict3[i]= dict2[d] #store values from dictionaries as key in new dictionary to pass for sorting
                        #j=j+1              #use incremental number value as placeholder for value in new dictionary (dict3)
        #print dict3

        sorted_dict = sorted(dict3.iteritems(), key=operator.itemgetter(1))
        #print len(sorted_dict)

        for k in range(len(sorted_dict)):
            index_list.append(sorted_dict[k][0])
            return_list.append(attr_list[index_list[k]])
        #print sorted_dict

        for h in range(len(attr_list)):
            if not(h in index_list):
                return_list.append(attr_list[h])

        #print return_list
        return return_list

#
    def sort_data (self,attr_list,sort_value_list):
        return_list = []
        #sort by uri
        if len(sort_value_list)==0:
            dict1= {}
            dict2= {}
            index_list = []
            for i in range(len(attr_list)):
                dict1 = attr_list[i] #parsed list of dictionaries into a new dictionary (dict1) {uri : {user_meta_key : user_meta_value}}
                for d in dict1:
                    dict2[i] = d
            #print dict2
            sorted_dict = sorted(dict2.iteritems(), key=operator.itemgetter(1))
            #print sorted_dict
            for k in range(len(sorted_dict)):
                index_list.append(sorted_dict[k][0])
                return_list.append(attr_list[index_list[k]])
            print return_list
        #
        if len(sort_value_list)>0:
            unsorted_list = []
            dict_index_list = []
            dict1 = {}
            for i in range(len(sort_value_list)):
                if i==0:
                    return_list=self.sort_data_helper (attr_list,sort_value_list[i])
                else:
                    dup_value_list=[]
                    dup_index_list = []
                    sort_value=sort_value_list[i]
                    #pares all dic that contains sort_value into unsorted_list
                    for j in range(len(return_list)):
                        #print i
                        dict1 = return_list[j] #parsed list of dictionaries into a new dictionary (dict1) {uri : {user_meta_key : user_meta_value}}
                        for d in dict1:
                            dict2 = dict1[d] #Use a new dictionary to {user_meta_key : user_meta_value}
                            if dict2.has_key(sort_value_list[i-1]):
                                if dict2[sort_value_list[i-1]] in dup_value_list:
                                    dup_index_list.append(dup_value_list.index(dict2[sort_value_list[i-1]]))
                                    dup_index_list.append(j)
                                else:
                                    dup_value_list.append(dict2[sort_value_list[i-1]])
                                #unsorted_list.append(dict1)
                                #dict_index_list.append(j)
                    print dup_index_list
                  #  print unsorted_list
                    #print return_list

