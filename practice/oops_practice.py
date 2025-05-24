class test:
    def __init__(self):
        self.__superprivate="Hello"
        self._superprivate="World"

my=test()
print(my._superprivate)
print(my.__dict__)