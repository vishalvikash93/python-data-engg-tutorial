import sqlite3

# Establish a database connection
conn = sqlite3.connect('database_name.db')

# Read data from SQL
df = pd.read_sql('SELECT * FROM table_name', conn)

# Display first few rows
print(df.head())

# Close the connection
conn.close()
