from __future__ import print_function
from bs4 import BeautifulSoup
import re
import pandas as pd
import pymysql


def get_housing_data(housing_list):
    house_data = []
    for link in housing_list:
        soup = get_data(link)
        address = get_address(soup)
        city = get_city(soup)
        state = get_state(soup)
        zip = get_zip(soup)
        beds = get_beds(soup)
        baths = get_baths(soup)
        sqft = get_sqft(soup)
        year_built = get_built_year(soup)
        listing_price = get_listing_price(soup)
        zestimate = get_zestimate(soup)
        house_data.append([address, city, state, zip, beds, baths, sqft, year_built, listing_price, zestimate])

    columns = ['Address', 'City', 'State', 'Zip', 'Beds', 'Baths', 'Sqft', 'Year_Built', 'Listing_Price', 'Zestimate']
    house_data_df = pd.DataFrame(house_data, columns=columns)
    return house_data_df


# Getting data from a html file
def get_data(html):

    f = open(html, encoding="utf8")
    bs_obj = BeautifulSoup(f, 'html.parser')
    f.close()
    return bs_obj

# Extract data field:
# Address, City, Zip, State, #Beds, #Baths,
# sqft, year built, listing price, zestimate


def get_address(bsObj):
    try:
        obj = bsObj.find('h1', class_="ds-address-container").find('span').get_text().split(',')
        address = obj[0]
        return address
    except:
        return None


def get_city(bsObj):
    try:
        obj = bsObj.find('h1', class_="ds-address-container").find_all('span')[1].get_text().split(',')
        city = re.split("\xa0", obj[0])[1]
        return city
    except:
        return None


def get_state(bsObj):
    try:
        obj = bsObj.find('h1', class_="ds-address-container").find_all('span')[1].get_text().split(',')
        state = obj[1].split(' ')[1]
        return state
    except:
        return None


def get_zip(bsObj):
    try:
        obj = bsObj.find('h1', class_="ds-address-container").find_all('span')[1].get_text().split(',')
        zip = obj[1].split(' ')[2]
        return zip
    except:
        return None


def get_beds(bsObj):
    try:
        obj = bsObj.find_all('span', class_="ds-bed-bath-living-area")[0].get_text()
        bed = obj[0]
        return bed
    except:
        return None


def get_baths(bsObj):
    try:
        obj = bsObj.find_all('span', class_="ds-bed-bath-living-area")[1].get_text()
        bath = obj[0]
        return bath
    except:
        return None


def get_sqft(bsObj):
    try:
        obj = bsObj.find_all('span', class_="ds-bed-bath-living-area")[2].get_text()
        sqft = obj.split(' ')[0]
        return sqft
    except:
        return None


def get_house_type(bsObj):
    try:
        house_type = bsObj.find_all('span', class_="ds-body ds-home-fact-value")[0].get_text()
        return house_type
    except:
        return None


def get_built_year(bsObj):
    try:
        built_year = bsObj.find_all('span', class_="ds-body ds-home-fact-value")[1].get_text()
        return built_year
    except:
        return None


def get_listing_price(bsObj):
    try:
        listing_price = bsObj.find_all('span', class_="ds-value")[0].get_text()
        return listing_price
    except:
        return None


def get_zestimate(bsObj):
    try:
        obj = bsObj.find_all("p")[0].get_text().split(' ')
        zestimate = re.split('\xa0', obj[1])[1]
        return zestimate
    except:
        return None


def export_to_csv(data, filename):
    data.to_csv(filename, index=False)


def export_to_db(data):
    """Export to database"""

    # Connect to the database using pymsql.connect()
    db_connection = pymysql.connect(
    host="localhost",
    user="root",
    password="skysh@2018",
    db="housing")

    # Create cursor
    cursor = db_connection.cursor()

    # Create table zillow_housing_data
    drop_table = "DROP TABLE IF EXISTS zillow_housing_data"
    cursor.execute(drop_table)

    create_table = "CREATE TABLE IF NOT EXISTS zillow_housing_data (Address CHAR(100) UNIQUE, \
                    City VARCHAR(100), State VARCHAR(100),  Zip INT, Beds INT, \
                    Baths FLOAT, Sqft VARCHAR(30), Year_Built INT, \
                    Listing_Price VARCHAR(30), Zestimate VARCHAR(30))"
    cursor.execute(create_table)

    # Insert DataFrame records one by one
    try:
        for i, row in data.iterrows():
            sql = "INSERT INTO zillow_housing_data VALUES (" + "%s,"*(len(row)-1) + "%s)"

            cursor.execute(sql, tuple(row))

            # The connection is not autocommited by default, so must commit to save the changes
            db_connection.commit()

    except cursor.IntegrityError:
        print("Duplicate entry!")

    # # Execute query to make sure that inserted data has been saved correctly
    # cursor.execute("SELECT * FROM zillow_housing_data")
    #
    # # Fetch all the records
    # result = cursor.fetchall()
    # for i in result:
    #     print(i)

    db_connection.close()


if __name__ == "__main__":
    house_list = ["/Users/santanusarma/Dropbox/Jagriti/Programming/Data Analysis/data_parsing/html_parsing/data/raw/zillow_7325_92126.html",
                  "/Users/santanusarma/Dropbox/Jagriti/Programming/Data Analysis/data_parsing/html_parsing/data/raw/zillow_8654_92126.html",
                  "/Users/santanusarma/Dropbox/Jagriti/Programming/Data Analysis/data_parsing/html_parsing/data/raw/zillow_9439_92126.html",
                  "/Users/santanusarma/Dropbox/Jagriti/Programming/Data Analysis/data_parsing/html_parsing/data/raw/zillow_10226_92126.html",
                  "/Users/santanusarma/Dropbox/Jagriti/Programming/Data Analysis/data_parsing/html_parsing/data/raw/zillow_11550_92126.html"]

    file_name = "/Users/santanusarma/Dropbox/Jagriti/Programming/Data Analysis/data_parsing/html_parsing/data/structured/housing_data.csv"

    housing_data = get_housing_data(house_list)
    export_to_csv(housing_data, file_name)
    export_to_db(housing_data)
    print(housing_data)


