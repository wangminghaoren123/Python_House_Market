# Ensure that you have both beautifulsoup and requests installed:
#   pip install beautifulsoup4
#   pip install requests

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import pyodbc
from pandas import DataFrame
import os


def Booli_findNumberOfPagesData(url):
    """
    Loading html object from given url and finds
    the number of pages and total number of objects (apartments)

    input: url string
    output: number of pages and objects
    """
    
    result = requests.get(url)
    print(url)
    soup = BeautifulSoup(result.text,'lxml')
    data = soup.findAll('div',class_ = 'search-list__pagination-summary')
    print(data)
    print(len(data))

    numberOfObjectsPerPage = 34
    try:
        numberOfObjects = int(data[0].text[-(len(data[0].text)-3 - data[0].text.rfind("av")):])
    except:
        numberOfObjects = 0

    numberOfPages = int(np.ceil(numberOfObjects/numberOfObjectsPerPage))
    
    return numberOfPages, numberOfObjects

def Booli_ScrapeObjects(page, object_info):
    """
    Scraping specific information for each object

    input: url, predefined, empty dataframe
    output: dataframe with object info (url, price, price/m2, number of rooms, m2, date sold, rent, drift cost, floor, built year)
    """

    result = requests.get(page)
    # To make sure that the website is accessible, we can
    # ensure that we obtain a 200 OK response to indicate
    # that the page is indeed present:
    print(result.status_code)
    print("..................the page .....................")
    
    print(page)
    print("..................the page......................")
    
    
    # Now, let us store the page content of the website accessed
    # from requests to a variable:
    # Now that we have the page source stored, we will use the
    # BeautifulSoup module to parse and process the source.
    # To do so, we create a BeautifulSoup object based on the
    # source variable we created above:
    src = result.content
    print("..................the soup......................")
    soup = BeautifulSoup(src,'lxml')
    links = soup.find_all("a")
    print(links)
    
    for link in links:
        if "/annons/" in link.text:
            print(link)
            objectLinks = link
   
    print (objectLinks)
    for i, row in enumerate(objectLinks):
        objectInfo = []
        
        #Summary information displayed for each object
        try:
            objectInfo.append(row.contents[5].text.split("\n")[1].strip(" kr"))                      #Price
            objectInfo.append(row.contents[5].text.split("\n")[2].strip(" kr/m²"))                   #Price/m2
            objectInfo.append(row.contents[3].text.split("\n")[2].split(",")[0].strip(" rum"))       #Number of rooms
            objectInfo.append(row.contents[3].text.split("\n")[2].split(",")[1].strip(" + m²"))      #m2
            objectInfo.append(row.contents[5].text.split("\n")[3])                                   #Date sold

            #Converting dates to english format
            if objectInfo[4].find("maj") != -1:
                objectInfo[4] = row.contents[5].text.split("\n")[3].replace('j', 'y')
            elif objectInfo[4].find("okt") != -1:
                objectInfo[4] = row.contents[5].text.split("\n")[3].replace('k', 'c')

        except:
            for j in range(0, 4):
                objectInfo.append("N/A")
            continue

        #Adding the object specific url
        objectInfo.insert(0, "https://www.booli.se" + objectLinks[i]["href"])

        #Requesting more granular information for each object
        try:
            request_apartment = requests.get(objectInfo[0])
            soup_apartment = BeautifulSoup(request_apartment.text, 'lxml')
            apartment_info = soup_apartment.findAll('ul',class_ = 'property__base-info__list property__base-info__list--sold')

            objectInfo.append(apartment_info[0].contents[5].text.split("\n")[2].strip(" kr/mån"))                 #rent
            objectInfo.append(apartment_info[0].contents[9].text.split("\n")[4].split("\t")[3].strip(" kr/mån"))  #Drift cost
            if apartment_info[0].contents[11].text.split("\n")[1].strip(" ") == "Våning":

                objectInfo.append(apartment_info[0].contents[11].text.split("\n")[2].strip(" "))                      #floor
                objectInfo.append(apartment_info[0].contents[13].text.split("\n")[2].strip(" "))                      #built year
            else:
                objectInfo.append("N/A")                                                                              #floor
                objectInfo.append(apartment_info[0].contents[11].text.split("\n")[2].strip(" "))                      #built year
        except:
            for j in range(0,2):
                objectInfo.append("N/A")
            continue

        object_info.append(objectInfo)
        
    return object_info

#LOOPING THROUGH SEARCH DATA AND ALL ITS PAGES
def loopThroughRegions(data_url):
    object_info = []
    region = []
    length = [0]

    for index, row in data_url.iterrows():
        #Base URL
        url ="https://www.booli.se/slutpriser/{}/{}/?maxSoldDate={}&minSoldDate={}&objectType=L%C3%A4genhet&page=1".format(row["Region"], row["RegionID"], searchDate_end, searchDate_start)
        object_info = Booli_ScrapeObjects(url, object_info) #Scraping function
        numberOfPages, numberOfObjects = Booli_findNumberOfPagesData(url) #Find number of pages for the given region

        #Looping through remaining pages (excluding 1 by starting at 2)
        for page in range(2, numberOfPages):
            url = "https://www.booli.se/slutpriser/{}/{}/?maxSoldDate={}&minSoldDate={}&objectType=L%C3%A4genhet&page={}".format(row["Region"], row["RegionID"], searchDate_end, searchDate_start, page)
            object_info = Booli_ScrapeObjects(url, object_info)

        length.append(len(object_info)) #How many object did I store for the given region?

        #Creating a simple vector containing duplicates of regions up to number of object stored for each region
        for i in range(0, length[len(length)-1] - length[len(length) - 2]):
            region.append(row["Region"])
   
    return object_info, region

def cleaningData(object_info):
    for index, row in object_info.iterrows(): 
        if row["m2"].find("+") != -1:
            m2s = row["m2"].split("+")
            newM2 = int(m2s[0]) + int(m2s[1])
            object_info.set_value(index, "m2", newM2)
        
        if row["Number of floors"] == "N/A":
            object_info.set_value(index, "Number of floors", 0)
        
        if row["Number of floors"] == "BV":
            object_info.set_value(index, "Number of floors", 0)
            
        if row["Number of floors"].find("½") != -1:
            floors = row["Number of floors"].split("½")
            if floors[0] == "":
                newFloors = 0.5
            else:
                newFloors = float(0.5) + float(floors[0])
                
            object_info.set_value(index, "Number of floors", newFloors)
        
        if row["Number of rooms"].find("½") != -1:
            rooms = row["Number of rooms"].split("½")
            if rooms[0] == "":
                newRooms = 0.5
            else:
                newRooms = float(0.5) + float(rooms[0])
            
            object_info.set_value(index, "Number of rooms", newRooms)
    
    return object_info

#INPUT DATA
headers= ['RegionID', 'Region']
data_url = {'RegionID': [874691],
            'Region': ['Bergshamra']}
data_url = pd.DataFrame(data_url, columns = headers)

searchDate_start = date.today() - timedelta(days=16)
searchDate_end = date.today()


#FETCHING DATA
object_info, region = loopThroughRegions(data_url)

#POLISHING THE DATA
headers = ['Link', 'Price', 'Price/m2', 'Number of rooms', 'm2', 'Date sold', 'Rent', 'Drift cost', 'Number of floors', 'Built year']
object_info = pd.DataFrame(object_info, columns = headers)
object_info['Region'] = region
object_info['Date sold'] = pd.to_datetime(object_info['Date sold'], format = '%d %b %Y')
object_info["Number of floors"] = object_info["Number of floors"].map(lambda x: x.rstrip(" tr"))
object_info["Price"] = object_info["Price"].map(lambda x: int("".join(x.split())))
object_info["Price/m2"] = object_info["Price/m2"].map(lambda x: int("".join(x.split())))
object_info["Rent"] = object_info["Rent"].map(lambda x: int("".join(x.split())))
object_info["Drift cost"] = object_info["Drift cost"].map(lambda x: int("".join(x.split())))

#SOME FURTHER POLISHING


object_info = cleaningData(object_info)
df_object_info = pd.DataFrame(object_info)
#df_object_info.to_csv(os.getcwd()+'\\3yeardata-Bergshamra.csv')

#data save to csv INPUT PARAMETERS
cwd = os.getcwd() 
print(cwd)
print("Done.")
