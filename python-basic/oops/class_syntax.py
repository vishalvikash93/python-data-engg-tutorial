#syntax
#
# class Name_of_class:
#     def __init__(self):
#         pass
from array import array


class Demo:
    def __init__(self,array,list):
        self.array=array
        self.list=list

    def method1(self):
        print("in method1")
        print(self.list)

onj_D=Demo([1,345,],[7,4,6])

onj_D.method1()