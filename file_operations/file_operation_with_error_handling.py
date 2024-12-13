try:
    with open('nonexistent_file.txt', 'r') as file:
        content = file.read()
except FileNotFoundError:
    print("The file does not exist.")
    print("Tryng to create file")
    # with open('nonexistent_file.txt', 'w') as file:
    #     file.write("Hello, World!\n")
    #     file.write("Welcome to file handling in Python.")
except IOError:
    print("An error occurred while handling the file.")


# with open('nonexistent_file.txt', 'r') as file:
#     content = file.read()