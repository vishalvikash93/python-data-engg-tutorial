def flatten_list(lst):
    finallist=[]
    for i in lst:
        if isinstance(i,list):
            finallist.extend(flatten_list(i))
        else:
            finallist.append(i)
    return finallist

nested_list = [[1, 2, [3, 4]], [5, 6], 7, [8, [9, 10]]]
flattened_list = flatten_list(nested_list)
print(flattened_list)
