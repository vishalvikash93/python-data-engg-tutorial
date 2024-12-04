str1 = "Hello"
str2 = "World"
result = str1 + " " + str2  # "Hello World"


str1 = "Hello"
result = str1 * 3  # "HelloHelloHello"



text = "Python"
first_char = text[4]  # 'P'
last_char = text[-5]  # 'n'

# print(last_char)


text = "Python"
substring = text[1:4]  # 'yth' (from index 1 to 3)
# print(substring)


text = "hello"
result = text.upper()  # 'HELLO'
# print(result)

text = "HELLO"
result = text.lower()  # 'hello'


#Converting the first character of each word to uppercase.
text = "hello world"
result = text.title()  # 'Hello World'



# inverts the case of all characters.python Copy code
text = "HeLLo WoRLd"
result = text.swapcase()
# print(result)

#Finding the index of a substring. python Copy code
text = "Hello World"
index = text.find("World")  # 6


#Checking if a substring exists. python Copy code
text = "Hello World Year"
result = "year" in text  # True


#Replacing occurrences of a substring with another.
text = "Hello World"
result = text.replace("World", "Python")  # 'Hello Python'
# print(result)

#Removing leading and trailing whitespaces.
text = "  Hello  "
result = text.strip()  # 'Hello'
# print(text)
# print(result)

#Removing leading or trailing whitespaces.

text = "  Hello  "
result = text.lstrip()  # 'Hello  '
result = text.rstrip()  # '  Hello'


#Padding the string.

text = "Hello"
result = text.center(10, '-')  # '--Hello---'
result = text.ljust(10, '-')  # 'Hello-----'
result = text.rjust(10, '-')  # '-----Hello'


#Splitting a string into a list of substrings based on a delimiter.

text = "apple,banana,orange"
result = text.split(",")  # ['apple', 'banana', 'orange']
# print(result)

#Joining elements of a list into a single string.

words = ['apple', 'banana', 'orange']
result = ",".join(words)  # 'apple, banana, orange'


#Using f-strings for inline variable interpolation (Python 3.6+).

name = "Alice1"
age = 30
result = f"Name: {name}, Age: {age}"  # 'Name: Alice, Age: 30'
# print(result)
#Using backslashes for special characters.

text = "This is a \"quote\"."
result = text  # 'This is a "quote".
# print(text)


# Checking if all characters are digits.

text = "12345"
result = text.isdigit()  # True
# print(result)


# Checking if all characters are alphabetic. ython

text = "Hello"
result = text.isalpha()  # True


#Checking if all characters are lowercase or uppercase.

text = "hello"
result = text.islower()  # True
text = "HELLO"
result = text.isupper()  # True



