def get_stats(numbers):
    total = sum(numbers)
    average = total / len(numbers)
    return total, average

# Calling the function
total, avg = get_stats([10, 20, 30, 40])
print(f"Total: {total}, Average: {avg}")
# Output: Total: 100, Average: 25.0
