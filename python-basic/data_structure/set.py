# Using curly braces
my_set = {1, 2, 3}
# lst=list(my_set)lst
# Using the set() constructor
my_set = set([1, 2, 3])

# dct={}
empty_set = set()  # Correct way to create an empty set

#Unordered: Sets do not guarantee the order of elements. When you print a set,
# the order of elements may vary.

my_set = {3, 1, 2}
print(my_set)  # Output: {1, 2, 3} (Order may vary)


#Adding Elements to a Set:
# You can add elements using the add() method.
my_set.add(5)
print(my_set)  # Output: {1, 2, 3, 4, 5}

#You can remove elements using the remove() or discard() methods.
# remove() will raise an error if the element doesn't exist in the set.
# discard() will not raise an error if the element is missing.

my_set.remove(3)
print(my_set)  # Output: {1, 2, 4, 5}

my_set.discard(10)  # No error even though 10 is not in the set


#Union (|): Combines elements from two sets.
set1 = {1, 2, 3}
set2 = {3, 4, 5}
union_set = set1 | set2
print(union_set)  # Output: {1, 2, 3, 4, 5}

#ntersection (&): Returns elements that are common to both sets.
intersection_set = set1 & set2
print(intersection_set)

#Difference (-): Returns elements in one set but not the other.
difference_set = set1 - set2
print(difference_set)  # Output: {1, 2}


#Symmetric Difference (^): Returns elements in either set, but not in both.
symmetric_diff_set = set1 ^ set2
print(symmetric_diff_set)  # Output: {1, 2, 4, 5}

#Set Comprehensions:
# You can also create sets using set comprehensions, similar to list comprehensions.
squared_set = {x**2 for x in range(5)}
print(squared_set)  # Output: {0, 1, 4, 9, 16}


#Set Membership:
# You can check if an element is in a set using the in keyword.
print(3 in my_set)  # Output: False
print(2 in my_set)  # Output: True

#terating Over a Set:
for item in my_set:
    print(item)

#Set Length:
print(len(my_set))  # Output: 4

