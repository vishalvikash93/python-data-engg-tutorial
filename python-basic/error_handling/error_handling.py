# Syntax
try:
    # Try to divide by zero (which will raise an exception)
    result = 10 / 0
    print(f'result:{result}')
except ZeroDivisionError:
    # Handle the specific exception (ZeroDivisionError)
    print("You cannot divide by zero!")
except  Exception as e:
    print(f"exception main:{e}")

else:
    # This block will run only if no exception occurs
    print("Division successful!")
finally:
    # This block always runs, regardless of any exceptions
    print("Execution complete!")


# result = 10 / 0
# print(f'result:{result}')



