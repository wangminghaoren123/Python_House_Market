# -*- coding: utf-8 -*-
"""
Created on Sun Oct  6 13:48:16 2019
#Data scrape through website
@author: wangming
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import pyodbc
from pandas import DataFrame
import os


def ScrapeObjects(page, object_info):
    """
    Search/find/scape information from each object

    input: url, predefined, empty dataframe
    output: dataframe with object info (url, price, price/m2, number of rooms, m2, date sold, rent, drift cost, floor, built year)
    """

    request = requests.get(page)
    print(page)
    soup = BeautifulSoup(request.text,'lxml')
   
    objectLinks = soup.select("a[href*=/annons/]")
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