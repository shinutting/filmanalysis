# -*- coding: utf-8 -*-
"""
Created on Mon Feb 28 19:17:16 2022

@author: Shin

Use this as a template for connecting to Selenium
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import pandas as pd
import numpy as np

import sqlite3

#Selenium
#Driver Settings
#------------------------------------------------------------------------------
options = Options()
options.headless = True
options.add_argument("--window-size=1920,1200")

DRIVER_PATH = r'C:\Users\Shin\Documents\ChromeDriver\chromedriver'
driver = webdriver.Chrome(options=options, executable_path=DRIVER_PATH)
print("Driver Activation Successful")
print("\n")
#------------------------------------------------------------------------------
#SQL
#Connecting to the database and creating a cursor object which allows SQL queries
#to be run
conn = sqlite3.connect('Movie_Review_DB.db')
c = conn.cursor()

#------------------------------------------------------------------------------

Top100_Links = 0

#Below code creates a new table within the .db file
append_table = False #Select True if collecting links for the first time
create_new_table = False #Select True if creating new table

if append_table:
    
    if create_new_table:
        c.execute('CREATE TABLE Top100 (Movie_Title,Link)')
        conn.commit()
    
    #get all links from main page
    driver.get('https://www.metacritic.com/browse/movies/score/metascore/year/filtered?view=condensed&year_selected=2021')
    movie_title = driver.find_elements_by_xpath("//*[@class='details']//a[@class='title']")

    #loop from links list and append values to a dataframe
    starting_list = []
    limit = 0
    for m in movie_title:
        Links_m = m.get_attribute("href")
        starting_list.append(Links_m)
        limit = limit+1
        if limit>99: #Change to 99 when not testing
            break

    #create dataframe for links to movies
    col_names = ["Movie_Title","Link"]
    
    Critic_Links_df = pd.DataFrame(columns = col_names)
    for s in starting_list:
        current_link = s
        driver.get(current_link)
        
        current_title = driver.find_element_by_xpath("//*[contains(@class,'product_page_title')]")
        current_title = current_title.text
        print("current_title")
        print(current_title)
        print("\n")        
        
        current_critic_link= driver.find_element_by_xpath("//*[@class='subsection_title']//a")
        critic_href = str(current_critic_link.get_attribute("href"))
        print("critic_href")
        print(critic_href)
        print("\n")
        
        s_list = [[current_title,critic_href]]
        s_df = pd.DataFrame(s_list, columns=col_names)
        Critic_Links_df = Critic_Links_df.append(s_df, ignore_index = True)

    print("Complete Dataframe - Top 100 Links:")
    print(Critic_Links_df)
    print("\n")
    
    
    #Append dataframe to SQL
    Critic_Links_df.to_sql('Top100',conn, if_exists='append', index=False)

else:
    #Read SQL DB to get list of previously identified movies   
    Top100_Links = pd.read_sql_query("select * from Top100", conn)
                       

#------------------------------------------------------------------------------

Top100_Ratings = 0

create_new_table2 = False
get_base_reviews = False

if get_base_reviews:
    if create_new_table2:
        c.execute('CREATE TABLE Top100_Ratings (Title,Critic,Rating,Critic_Link)')
    
    
    #create dataframe to store critic links
    review_table_col = ["Title","Critic","Rating","Critic_Link"]
    review_table_df = pd.DataFrame(columns = review_table_col)
    
    for row in range(len(Top100_Links)):
        current_critic_link = Top100_Links.loc[row,"Link"]
        
        current_Top100 = Top100_Links.loc[row,"Movie_Title"]
        driver.get(current_critic_link)
        
        review_container = driver.find_elements_by_xpath("//*[contains(@class, 'review pad_top1')]")
        
        for review in review_container:
            
            try:
                movie_critic = review.find_element_by_xpath(".//*[@class='author']")
                movie_critic_link = review.find_element_by_xpath(".//*[@class='author']//a")
                                    
                movie_critic_text = movie_critic.text
                print("critic:")
                print(movie_critic_text)
                
                movie_critic_href = str(movie_critic_link.get_attribute("href"))
                print("href:")
                print(movie_critic_href)
                
                movie_rating = review.find_element_by_xpath(".//*[contains(@class, 'metascore_w large')]")
                movie_rating = movie_rating.text
                print("rating")
                print(movie_rating)
                print("\n")
                
                a_list = [[current_Top100,movie_critic_text, movie_rating, movie_critic_href]]   
                a_df = pd.DataFrame(a_list,columns = review_table_col)  
                a_df.to_sql('Top100_Ratings',conn, if_exists='append', index=False)
                review_table_df = review_table_df.append(a_df,ignore_index = True)
            except:
                print("Error")
                print("\n")
            
        print(review_table_df)
    
    #this appends the total review_table_df to SQL, however each line is already appended in the loop above. 
    #review_table_df.to_sql('Top100_Ratings',conn, if_exists='append', index=False)

else:
    #Read SQL DB to get list of previously identified movies   
    Top100_Ratings = pd.read_sql_query("select * from Top100_Ratings", conn)
    Top100_Ratings = Top100_Ratings.drop_duplicates('Critic_Link')

    
#------------------------------------------------------------------------------
#This is the main data collection point for this project

critic_table_col =["Page_Number","Critic","Movie_Title","Rating","Movie_Link"]
critic_table_df = pd.DataFrame(columns = critic_table_col)

create_new_table3 = False
if create_new_table3:
    c.execute('CREATE TABLE Critic_Analysis_v2 (Page_Number,Critic,Movie_Title,Rating,Movie_Link)')

page_number = 0 #0 is main page   
page_ref = "?filter=movies&page=" + str(page_number)

run_analysis = False
counter = 1
max_counter = len(Top100_Ratings)
if run_analysis:
    for index, row in Top100_Ratings.iterrows():    
    #for critic in range(len(Top100_Ratings)):
        current_critic_name = row["Critic"]
        current_critic_link = row["Critic_Link"]
        
        driver.get(current_critic_link)
        
        try:
            #Find page number
            if page_number>0:
                page_link = driver.find_element_by_xpath("//*[contains(@href, '%s')]" % page_ref)
                page_href = str(page_link.get_attribute("href"))
    
                driver.get(page_href)
                critic_container = driver.find_elements_by_xpath("//*[contains(@class,'review critic_review')]")
            
            else:
                critic_container = driver.find_elements_by_xpath("//*[contains(@class,'review critic_review')]")
            
            for title in critic_container:
                try:
                    movie_critic_title = title.find_element_by_xpath(".//*[@class='review_product']//a")
                    movie_critic_rating = title.find_element_by_xpath(".//*[contains(@class,'metascore_w small movie')]") 
                    movie_critic_href = str(movie_critic_title.get_attribute("href"))
                    
                    c_list = [[page_number,current_critic_name,movie_critic_title.text, movie_critic_rating.text, movie_critic_href]]   
                    c_df = pd.DataFrame(c_list,columns = critic_table_col)  
                    c_df.to_sql('Critic_Analysis_v2',conn, if_exists='append', index=False)
                    critic_table_df = critic_table_df.append(c_df,ignore_index = True)
                       
                except:
                    print("Error")
                    print("\n")
            print(str(counter) + "/" + str(max_counter))
            counter = counter +1
        except:
            print("page missing - " + str(counter))
            counter = counter +1  
        #print(critic_table_df)
       
#------------------------------------------------------------------------------
#Find categories for each title

category_table_col =["Movie_Title","Movie_Link","Movie_Category"]

create_new_table4 = True
if create_new_table4:
    c.execute('CREATE TABLE Top100_Categories (Movie_Title,Movie_Link, Movie_Category)')

#Read SQL DB to get list of previously identified movies   
all_titles = pd.read_sql_query("select * from Top100_New", conn)
#all_titles = all_titles.drop_duplicates('New_Link')

find_categories = True
counter = 0

if find_categories:
    for index, row in all_titles.iterrows():    
        current_title = row["Movie_Title"]
        current_title_link = row["New_Link"]
        current_title_link_filler = row["Link"]
        
        driver.get(current_title_link)
        try:
            movie_detail = driver.find_element_by_xpath("//*[contains(@class,'section_title')]//a")
            movie_detail_href = str(movie_detail.get_attribute("href"))
            
            driver.get(movie_detail_href)
            cat_genre = driver.find_element_by_xpath("//*[contains(@class,'genres')]//*[contains(@class,'data')]")
            
            cat_list = [[current_title, current_title_link_filler, cat_genre.text]]   
            cat_df = pd.DataFrame(cat_list,columns = category_table_col)  
            cat_df.to_sql('Categories',conn, if_exists='append', index=False)
            counter+=1
            print(str(counter) + "/" + str(len(all_titles)))
            
        except:
            counter+=1
            print("missing: " + str(current_title))
driver.quit()
print("Complete")