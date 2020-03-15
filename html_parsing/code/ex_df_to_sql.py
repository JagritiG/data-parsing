# Import pandas
import pandas as pd
from sqlalchemy import create_engine

# Create dataframe
data = pd.DataFrame({
    'book_id': [12345, 12346, 12347],
    'title': ['Python Programming', 'Learn MySQL', 'Data Science Cookbook'],
    'price': [29, 23, 27]
})

print(data)


# Insert whole DataFrame into MySQL
engine = create_engine('mysql+pymysql://root:skysh@2018@localhost/testdb2')
db_connection = engine.connect()
data.to_sql('book_details', con=db_connection, if_exists='append', chunksize=500)

# Execute query to make sure that inserted data has been saved correctly
result = db_connection.execute("SELECT * FROM book_details")
for i in result:
    print(i)

db_connection.close()
engine.dispose()
