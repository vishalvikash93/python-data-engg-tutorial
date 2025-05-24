s = "ab11cdabc5bb55"
vowel="aeiou"
# res=[i for i in s if i in vowel]


res=[s[i:j]  for i in range(len(s)) for j in range(i+1,len(s)) if s[i] in vowel and s[i:j].isalpha()  ]
print(res)