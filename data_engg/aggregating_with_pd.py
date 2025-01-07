import pandas as pd

data = {
    'Category': ['A', 'A', 'B', 'B', 'A', 'B'],
    'Value': [10, 20, 30, 40, 50, 60]
}
df = pd.DataFrame(data)

####group by the Category and calculate the sum of Value

grouped = df.groupby('Category')['Value'].sum()
print(grouped)


########multiple aggregation functions
grouped = df.groupby('Category')['Value'].agg(['sum', 'mean', 'count'])
print(grouped)


###########  apply different aggregations to different columns
grouped = df.groupby('Category').agg({
    'Value': 'sum',       # Sum of 'Value'
    'Amount': 'mean'      # Mean of 'Amount'
})
print(grouped)


####### custom aggression function
grouped = df.groupby('Category')['Value'].agg(lambda x: x.max() - x.min())
print(grouped)


####### Multiple custom aggression

grouped = df.groupby('Category')['Value'].agg([lambda x: x.max() - x.min(), 'mean', 'sum'])
print(grouped)

grouped = df.groupby('Category').agg({
    'Value': 'sum',        # Sum of Value
    'Amount': 'mean'       # Mean of Amount
})
print(grouped)

######### handeling missing data during grouping

df = pd.DataFrame({
    'Category': ['A', 'A', 'B', 'B', 'A', 'B'],
    'Value': [10, None, 30, 40, 50, None],
    'Amount': [100, 200, 300, 400, None, 600]
})

# Fill NaN values
df['Value'] = df['Value'].fillna(0)

grouped = df.groupby('Category').agg({
    'Value': 'sum',        # Sum of Value, filling NaN with 0
    'Amount': 'mean'       # Mean of Amount
})

print(grouped)



#######resetting index after aggression
grouped = df.groupby('Category')['Value'].sum().reset_index()
print(grouped)

