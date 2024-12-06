fruits = ["apple", "banana", "cherry"]
print(fruits[0])  # Output: apple
print(fruits[1])  # Output: banana
print(fruits[-1])  # Output: cherry (negative index starts from the end)

#You can modify an element in a list by referencing its index.

fruits[1] = "orange"  # Changes 'banana' to 'orange'
print(fruits)  # Output: ['apple', 'orange', 'cherry']


fruits.append("grape")  # Adds 'grape' to the end
print(fruits)  # Output: ['apple', 'orange', 'cherry', 'grape']

fruits.insert(1, "kiwi")  # Inserts 'kiwi' at index 1
print(fruits)  # Output: ['apple', 'kiwi', 'orange', 'cherry', 'grape']

fruits.extend(["pear", "melon"])  # Adds two items from another list
print(fruits)  # Output: ['apple', 'kiwi', 'orange', 'cherry', 'grape', 'pear', 'melon']

########## remove #######

fruits.remove("orange")  # Removes 'orange'
print(fruits)  # Output: ['apple', 'kiwi', 'cherry', 'grape', 'pear', 'melon']

popped_item = fruits.pop(2)  # Removes and returns item at index 2
print(popped_item)  # Output: cherry
print(fruits)  # Output: ['apple', 'kiwi', 'grape', 'pear', 'melon']

del fruits[1]  # Deletes item at index 1
print(fruits)  # Output: ['apple', 'grape', 'pear', 'melon']

fruits.clear()  # Clears the entire list
print(fruits)  # Output: []


########## list len #######
fruits = ["apple", "banana", "cherry"]
print(len(fruits))  # Output: 3

####### checking item in list ###########
fruits = ["apple", "banana", "cherry"]
print("apple" in fruits)  # Output: True
print("orange" in fruits)  # Output: False


######### slicing a list #######
fruits = ["apple", "banana", "cherry", "date", "elderberry"]
print(fruits[1:3])  # Output: ['banana', 'cherry'] (from index 1 to 2)
print(fruits[:2])  # Output: ['apple', 'banana'] (from start to index 1)
print(fruits[3:])  # Output: ['date', 'elderberry'] (from index 3 to end)

############ sorting

numbers = [5, 2, 9, 1, 5, 6]
numbers.sort()  # Sorts in ascending order
print(numbers)  # Output: [1, 2, 5, 5, 6, 9]

# Sort in descending order
numbers.sort(reverse=True)
print(numbers)  # Output: [9, 6, 5, 5, 2, 1]

# Using sorted() to create a sorted copy
numbers_copy = sorted(numbers)
print(numbers_copy)  # Output: [1, 2, 5, 5, 6, 9]

############ list comprehension ##########
# Example: Creating a list of squares
squares = [x**2 for x in range(1, 6)]
print(squares)  # Output: [1, 4, 9, 16, 25]


#Lists can contain other lists as elements, forming multi-dimensional arrays.

matrix = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
print(matrix[0])  # Output: [1, 2, 3]
print(matrix[1][2])