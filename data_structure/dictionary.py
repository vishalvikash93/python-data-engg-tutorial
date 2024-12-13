from data_structure.set import my_set

my_dict = {"key1": "value1", "key2": "value2", "key3": "value3"}

#Accessing Values: Values are accessed by referencing their associated keys.
my_dict = {"name": "Alice", "age": 25}
print(my_dict["name"])  # Output: Alice


#Uniqueness of Keys: Keys in a dictionary are unique.
# If you attempt to add a key that already exists, the existing value for that key will be updated.

my_dict = {"name": "Alice", "age": 25}
my_dict["age"] = 26  # Updates the value of "age"
print(my_dict)  # Output: {'name': 'Alice', 'age': 26}

#Data Types for Keys and Values:
# Keys: Can be any immutable type like strings, integers, or tuples.
# Values: Can be any data type, including lists, dictionaries, or even other tuples.

my_dict = {
    1: "apple",
    2: [1, 2, 3],
    (1, 2): "tuple"
}

#Adding or Updating Key-Value Pairs:
# You can add a new key-value pair to a dictionary, or update an existing one by using the key.

my_dict["city"] = "New York"  # Adding a new pair
my_dict["age"] = 27


#emoving Key-Value Pairs:
# Use del or pop() to remove key-value pairs.
# del removes a key-value pair by key:

del my_dict["city"]
# pop() removes the key-value pair and returns the value:

value = my_dict.pop("age")
print(value)  # Output: 27

#Accessing Keys, Values, and Items
keys = my_dict.keys()      # Returns dict_keys(['name'])
values = my_dict.values()  # Returns dict_values(['Alice'])
items = my_dict.items()


#Checking for Existence of Keys:
# Use the in keyword to check if a key exists in a dictionary.
if "name1" in my_dict:
    print("Name exists!")


#Dictionary Comprehension:
# You can create a dictionary using dictionary comprehension, similar to list comprehension.
squares = {x: x**2 for x in range(5)}
print(squares)  # Output: {0: 0, 1: 1, 2: 4, 3: 9, 4: 16}

#final example

student = {
    "name": "John Doe",
    "age": 20,
    "courses": ["Math", "Science"],
    "is_graduated": False
}

# Accessing a value by key
print(student["name"])  # Output: John Doe

# Modifying a value
student["age"] = 21

# Adding a new key-value pair
student["GPA"] = 3.8

# Checking if a key exists
if "is_graduated" in student:
    print("Graduation status exists.")  # Output: Graduation status exists.
