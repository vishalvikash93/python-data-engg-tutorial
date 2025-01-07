import pandas as pd

# df.groupby('column_name')




# data = {
#     'Category': ['A', 'B', 'A', 'B', 'A', 'B'],
#     'Value': [10, 20, 30, 40, 50, 60]
# }
# df = pd.DataFrame(data)



#
# data = {
#     'Category': ['A', 'B', 'A', 'B', 'A', 'B'],
#     'Value': [10, 20, 30, 40, 50, 60]
# }
# df = pd.DataFrame(data)


###group by multiple columns
data = {
    'Category': ['A', 'A', 'B', 'B', 'A', 'B'],
    'Subcategory': ['X', 'Y', 'X', 'Y', 'Y', 'X'],
    'Value': [10, 20, 30, 40, 50, 60]
}
df = pd.DataFrame(data)

grouped = df.groupby(['Category', 'Subcategory'])['Value'].sum()
print(grouped)

#####multiple aggregation functions
grouped = df.groupby('Category')['Value'].agg(['sum', 'mean'])
print(grouped)



######Group and transform data   -> use transform() to perform operations on each group and return a DataFrame of the same shape as the original.
df['Value_transformed'] = df.groupby('Category')['Value'].transform('mean')
print(df)

######## Filter groups -> filter the groups based on conditions. For example, if you only want groups where the sum of Value is greater than a certain threshold
grouped = df.groupby('Category').filter(lambda x: x['Value'].sum() > 50)
print(grouped)


###Resetting the index after grouping->>>>>When you group by a column,
# the result will use the grouped column(s) as an index. If you want to reset this index and return a flat DataFrame, you can use reset_index():

grouped = df.groupby('Category')['Value'].sum().reset_index()
print(grouped)

