
def merge(left,right):
    list=[]
    i,j=0,0
    while i<len(left) and j<len(right):
        if left[i]<right[j]:
            list.append(left[i])
            i+=1
        else:
            list.append(right[j])
            j+=1
    list.extend(left[i:])
    list.extend(right[j:])
    return list

def merge_sort(lst):
    if len(lst)<=1:
        return lst
    n=len(lst)
    m=n//2


    left=lst[:m]
    right=lst[m:]

    left=merge_sort(left)
    right=merge_sort(right)
    return merge(left,right)


arr = [38, 27, 43, 3, 9, 82, 10]
print(merge_sort(arr))