try:
    x = int("string")  # Will raise a ValueError
except (ValueError, TypeError) as e:
    print(f"An error occurred: {e}")
