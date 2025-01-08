import pandas as pd

# Sample DataFrame
data = {'Name': ['Alice', 'Bob', 'Alice', 'Dave', 'Bob'],
        'Age': [25, 30, 25, 40, 30]}
df = pd.DataFrame(data)

# Removing duplicate rows
df_unique = df.drop_duplicates()

print(df_unique)



# Removing duplicate rows based on the 'Name' column
df_unique = df.drop_duplicates(subset=['Name'])

print(df_unique)
