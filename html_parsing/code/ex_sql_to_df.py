# Read sql table into panda Dataframe
from sqlalchemy import create_engine
import pandas as pd

# db_connection_str = 'mysql+pymysql://root:skysh@2018@localhost/housing'
engine = create_engine('mysql+pymysql://root:skysh@2018@localhost/housing')
db_connection = engine.connect()

df = pd.read_sql('SELECT Address, City, Zip, Beds, Baths, Year_Built, Listing_Price FROM zillow_housing_data', con=db_connection)
print(df.head())
db_connection.close()
engine.dispose()
