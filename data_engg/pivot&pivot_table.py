import pandas as pd

# Sample DataFrame
data = {'Date': ['2022-01-01', '2022-01-01', '2022-01-02'],
        'City': ['New York', 'Los Angeles', 'New York'],
        'Temperature': [32, 75, 30]}
df = pd.DataFrame(data)

# Pivoting the DataFrame
df_pivot = df.pivot(index='Date', columns='City', values='Temperature')

print(df_pivot)

# Sample DataFrame
data = {'Date': ['2022-01-01', '2022-01-01', '2022-01-02', '2022-01-02'],
        'City': ['New York', 'Los Angeles', 'New York', 'Los Angeles'],
        'Temperature': [32, 75, 30, 70]}
df = pd.DataFrame(data)

# Pivoting the DataFrame with aggregation
df_pivot_table = df.pivot_table(index='Date', columns='City', values='Temperature', aggfunc='mean')

print(df_pivot_table)


# pivit_table grouper