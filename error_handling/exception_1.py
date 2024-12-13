try:
    # Try to divide by zero (which will raise an exception)
    result = 10 / 2
    print(f'result:{result}')
except  Exception as e:
    # Handle the specific exception (ZeroDivisionError)
    print(f"You cannot divide by zero!:{e}")