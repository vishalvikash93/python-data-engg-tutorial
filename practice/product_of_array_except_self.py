def maximum_difference(nums):
    if len(nums) < 2:
        return None  # If there are less than 2 elements, there's no pair

    nums.sort()  # Sort the list to bring elements together
    print(f'List after sorting: {nums}')

    pair = None  # To store the pair with the maximum difference
    max_diff = float('-inf')  # Initialize to a very small value to find maximum difference

    for i in range(0, len(nums) - 1):  # Iterate until the second-to-last element
        diff = nums[i+1] - nums[i]  # Calculate the difference between consecutive elements

        if diff > max_diff:  # Update the maximum difference and pair
            max_diff = diff
            pair = (nums[i], nums[i+1])  # Store the pair with the maximum difference

    return pair, max_diff  # Return the pair with the maximum difference and the diff itself
nums=[10, 2, 5, 15, 8]
print(maximum_difference(nums))