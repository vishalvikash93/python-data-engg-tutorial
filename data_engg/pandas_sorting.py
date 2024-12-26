import pandas as pd

# Example DataFrame
data = {'Name': ['Alice', 'Bob', 'Charlie', 'David'],
        'Age': [25, 30, 35, 40],
        'Salary': [70000, 80000, 120000, 100000]}

df = pd.DataFrame(data)

# Sort by a single column (ascending order)
df_sorted = df.sort_values(by='Age')

# Sort by multiple columns (ascending order)
df_sorted_multiple = df.sort_values(by=['Age', 'Salary'])

# Sort by a column in descending order
df_sorted_desc = df.sort_values(by='Salary', ascending=False)

print(df_sorted)
print(df_sorted_multiple)
print(df_sorted_desc)
