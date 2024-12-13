 # use write() to write a string to a file. This will overwrite the file’s contents if it already exists.
with open('example.txt', 'w') as file:
    file.write("Hello, World!\n")
    file.write("Welcome to file handling in Python.")


# If you want to add content to the end of an existing file without overwriting it, use the a mode.
with open('example.txt', 'a') as file:
    file.write("\nAppended text to the file.")



# use writelines() to write multiple lines at once
lines = ["First line\n", "Second line\n", "Third line\n"]

with open('example.txt', 'w') as file:
    file.writelines(lines)
