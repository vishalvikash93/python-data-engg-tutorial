try:
    # Try opening a file that doesn't exist
    file = open('non_existent_file.txt', 'r')
except FileNotFoundError:
    print("File not found!")
except IOError:
    print("I/O error occurred!")
finally:
    print("Finished.")
