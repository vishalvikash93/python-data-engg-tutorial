import pandas as pd

# Read CSV file
df = pd.read_csv('data/weather_data_with_missing_value.csv')

print(df)
# new_df=df.fillna(0)
#
# print(new_df)
#
# new_df=df.fillna({
#     'WindSpeed':0,
#     "Temperature":0,
#     "Event":"no event"
# })

# forward fill means if null value fill with previous days value
df.ffill()

# backword fill means if null value fill with previous days value
df.bfill()

print(df)



# df.ffill(inplace=True)  # Forward fill and modify the DataFrame in place
# # or
# df.bfill(inplace=True)  # Backward fill and modify the DataFrame in place


import numpy as np
# if suppose temperature value is invalid like -999999999999999 or 999999999999999999
df.replace(-999999999999999,np.nan)
df.replace([-999999999999999,-988888],np.nan)


# Handle multiple missing value
# df.replace({
#     "Temprature":-99999,
#     "Event":-88888888
# },np.nan)


