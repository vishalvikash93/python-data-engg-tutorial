def longest_substring(s: str) -> str:
    dct = {}
    max_lenght = 0
    max_substring = ""
    start = 0
    for i in range(len(s)):
        if s[i] in dct and dct[s[i]]>=start:
            start=dct[s[i]]
        dct[s[i]]=i
        if i-start+1>max_lenght:
            max_lenght=i-start+1
            max_substring=s[start:i+1]
    return max_substring



# Example usage
input_str = "abcdabcbb"
print("Longest substring without repeating characters:", longest_substring(input_str))
