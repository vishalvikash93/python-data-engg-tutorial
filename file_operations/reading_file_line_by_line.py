with open('example.txt', 'r') as file:
    line = file.readline()  # Reads the first line   readline() reads one line at a time.
    print(line)


# readlines() returns a list of lines.
with open('example.txt', 'r') as file:
    lines = file.readlines()
    for line in lines:
        print(line.strip())  # .strip() removes leading/trailing whitespace
