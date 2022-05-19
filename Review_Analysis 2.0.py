# -*- coding: utf-8 -*-
"""
Created on Sun Apr 17 17:53:10 2022

@author: Shin
"""

import pandas as pd
import numpy as np
import scipy.stats as stats

import sqlite3

#------------------------------------------------------------------------------
#SQL
#Connecting to the database and creating a cursor object which allows SQL queries
#to be run
conn = sqlite3.connect('Movie_Review_DB.db')
c = conn.cursor()

#------------------------------------------------------------------------------
#Set critic names & Categories

critic_names = pd.read_sql_query("SELECT * FROM critic_names", conn)
category_list = pd.read_sql_query("SELECT DISTINCT(Category2) FROM New_Categories", conn)

#------------------------------------------------------------------------------
#Create new SQL table

create_new_table = False
if create_new_table:
    c.execute('CREATE TABLE Ratings_Normalised_by_critics_genre (Title, Critic, Category2, Rating, Z_Score)')


#------------------------------------------------------------------------------
#Read SQL DB to get list of previously identified movies   
counter = 0
for index, row in critic_names.iterrows():    
        current_critic = row["Critic"]  
        try:        
            for index, row in category_list.iterrows():
                current_category = row["Category2"]
            
                SQL_query = "SELECT A.Title, A.Critic, N.Category2, A.Rating AS Rating  \
                            FROM All_Ratings_v2 AS A \
                            LEFT JOIN Categories AS C \
                            ON A.Title = C.Movie_Title \
                            LEFT JOIN New_Categories AS N \
                            ON N.Category = C.Movie_Category \
                            WHERE A.Critic IS '%s' \
                            AND N.Category2 IS '%s' " % (current_critic, current_category)
                
                stored_movies = pd.read_sql_query(SQL_query ,conn) 
                stored_movies = stored_movies.astype({'Rating':'float'})
                
                stored_movies['z_score'] = stats.zscore(stored_movies['Rating'])
    
                stored_movies.to_sql('Ratings_Normalised_by_critics_genre',conn, if_exists='append', index=False)
                
        except:
            print(current_critic)
        counter +=1
        #if counter>0:
        #    break
print("Code executed")