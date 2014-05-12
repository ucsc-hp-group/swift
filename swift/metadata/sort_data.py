from collections import OrderedDict
import operator


class Sort_metadata():
    def sort_data_helper(self, attr_list, sort_value):
        """
        Unitary function to help main function to sort one value at a time
        param attr_list:The list of unsorted dictionaries of custom metadata
        param sort_value: the sorting attribute set by user
        returns: The list of sorted dictionaries
        """
        dict1 = {}
        dict2 = {}
        dict3 = {}
        index_list = []
        return_list = []
        j=0
        h=0

        """Default: if no set sort_value then sort by uri"""
        if sort_value == "uri":
            for i in range(len(attr_list)):
                dict1 = attr_list[i]
                """parsed list of dictionaries into a new dictionary"""
                for d in dict1:
                    dict2[i] = d
            sorted_dict = sorted(dict2.iteritems(), key=operator.itemgetter(1))
            for k in range(len(sorted_dict)):
                index_list.append(sorted_dict[k][0])
                return_list.append(attr_list[index_list[k]])
            return return_list

        """sort_value defined: sort by attribute"""
        for i in range(len(attr_list)):
            """parsed list of dictionaries into a new dictionary """
            dict1 = attr_list[i]
            for d in dict1:
                dict2 = dict1[d]
                for d in dict2:
                    """Extract by only sort_value parameter (key)"""
                    if d == sort_value:
                        dict3[i]= dict2[d]
                        """store values from dictionaries as key in new dictionary to pass for sorting"""
        sorted_dict = sorted(dict3.iteritems(), key=operator.itemgetter(1))

        """Get indexes of the targetted entry for sorted attributes from the original list """
        for k in range(len(sorted_dict)):
            index_list.append(sorted_dict[k][0])
            return_list.append(attr_list[index_list[k]])

        """Appending the sorted attributes into original dictionary into the right indexes"""
        for h in range(len(attr_list)):
            if not(h in index_list):
                return_list.append(attr_list[h])

        return return_list

#
    def sort_data (self,attr_list,sort_value_list):
        """
        Sorts custom metadata by more than one sorting attributes given by user
        Param attr_list: The list of unsorted dictionaries of custom metadata
        Param sort_value_list: List of more than one sorting attribute given by user
        Returns: The list of sorted dictionaries by more than one sorting attribute
        """

        return_list = []
        if sort_value_list == ['']:
            return_list=self.sort_data_helper (attr_list,"uri")
        #
        if len(sort_value_list)>0:
            unsorted_list = []
            dup_value_dict = {}
            dict1 = {}
            dict2 = {}
            dict3 = {}
            k = 0;
            for i in range(len(sort_value_list)):
                if i==0:
                    return_list=self.sort_data_helper(attr_list,sort_value_list[i])
                else:
                    dup_value_list=[]
                    dup_index_list = []
                    sort_value=sort_value_list[i]
                    for j in range(len(return_list)):
                        dict1 = return_list[j]
                        for d in dict1:
                            dict2 = dict1[d]
                            if dict2.has_key(sort_value_list[i-1]):
                                if dict2[sort_value_list[i-1]] in dup_value_dict:
                                    dup_value_dict[dict2[sort_value_list[i-1]]].append(j)
                                else:
                                    dup_value_dict[(dict2[sort_value_list[i-1]])]= [j]
                    """sort only duplicate attributes extracted from sort_data_helper based on multiple sorting parameters"""
                    for key in dup_value_dict:
                        if len(dup_value_dict[key])>1:
                            ind_list = dup_value_dict[key]
                            unsort_value_list = []
                            for k in range(len(ind_list)):
                                unsort_value_list.append(return_list[ind_list[k]])
                                sorted_value_list = self.sort_data_helper(unsort_value_list,sort_value_list[i])
                            """Add the sorted list back to original dictionary based on indexes"""
                            for h in range(len(sorted_value_list)):
                                return_list[ind_list[h]]=sorted_value_list[h]
        return return_list
