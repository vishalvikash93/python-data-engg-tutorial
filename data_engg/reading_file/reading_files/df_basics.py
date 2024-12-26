import pandas as pd

# Read CSV file
df = pd.read_csv('data/weather_data.csv')

print(df.head())

df.tail()

df.head(2)

df[2:5]

# df.columns
#
# print(df.Temperature.std())
print(df.describe())
# df.describe()

# print(df[df.Temperature>2])
# print(df[df.Temperature==df['Temperature'].max()])


df.set_index("Event",inplace=True)
print(df)

# list out row using new index event as snow
print(df.loc['Snow'])