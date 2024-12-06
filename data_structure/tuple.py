my_tuple = (1, 2, 3, 4)


my_tuple = (1, "hello", 3.14, [1, 2, 3])


#Dictionary Keys: Tuples can be used as keys in a dictionary, whereas lists cannot.
# This is because dictionary keys need to be hashable, and tuples are immutable,
# making them hashable.

location_dict = { (1, 2): "Point A", (3, 4): "Point B" }



#Functions can return multiple values packed in a tuple.
# This is a common Python idiom used for returning more than one value from a function.

def get_min_max(numbers):
    return (min(numbers), max(numbers))

result = get_min_max([3, 1, 4, 1, 5])
print(result)

#Packing and Unpacking:
# You can "pack" multiple values into a tuple and then "unpack" them back into variables.
# This is a convenient way to handle multiple return values or arguments.

coordinates = (10, 20)
x, y = coordinates

