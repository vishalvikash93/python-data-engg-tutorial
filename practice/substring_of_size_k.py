def find_substring_after_digit(s: str, k: int) -> str:
    for i in range(len(s) - k + 1):
        if s[i].isdigit() and s[i+1].isalpha():
            return s[i+1:i+1+k]
    return ""

input_str = "ab11cdabc5bb55"
k=3
print("Longest substring without repeating characters:", find_substring_after_digit(input_str,k))