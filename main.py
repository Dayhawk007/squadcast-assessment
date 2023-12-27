import psycopg2
import csv
from datetime import datetime

#Connecting to the database and using local database because render doesn't have superuser rights for importing csv

conn = psycopg2.connect(database="movies_ratings", 
                        user='postgres', password='1234',  
                        host='127.0.0.1', port='5432'
) 

conn.autocommit = True
cursor = conn.cursor()


#Function to create both tables

def create_tables(cursor):

    creation_query_movies='''CREATE TABLE MOVIES(id int NOT NULL,
                            title varchar, 
                            year int,
                            country varchar,
                            genre varchar,
                            director varchar
                            ,minutes int,
                            poster varchar);'''
                            
    creation_query_ratings='''CREATE TABLE RATINGS(rater_id int NOT NULL,
                            movie_id int NOT NULL,
                            rating int,
                            time timestamp);'''

    cursor.execute(creation_query_movies)

    cursor.execute(creation_query_ratings)
    
    print("Tables created successfully........")

def insert_data(cursor):
    insertion_query_movies='''COPY movies FROM 'E:\Job_Assignments\squadcast-assessment\\movies.csv' DELIMITER ',' CSV HEADER;'''

    cursor.execute(insertion_query_movies)

    with open('ratings.csv', 'r') as ratings_csv:
        csv_reader = csv.reader(ratings_csv)
        
        for row in csv_reader:
            if(row[0]=='rater_id'):
                continue
            timestamp=datetime.utcfromtimestamp(int(row[3]))
            cursor.execute("INSERT INTO RATINGS (rater_id,movie_id,rating,time) VALUES (%s,%s,%s,%s)", (row[0],row[1],row[2],timestamp))   

    print("Data inserted successfully........")
    






#insertion_query_ratings='''COPY ratings(rater_id,movie_id,rating,time) FROM 'E:\Job_Assignments\squadcast-assessment\\ratings.csv' CSV HEADER WITH (FORMAT 'csv', DELIMETER ',', Quote '"', NULL 'NULL',TIMEFORMAT 'epoch');'''


sorting_query_by_duration='''SELECT title,minutes FROM movies ORDER BY minutes DESC LIMIT 5;'''

result=cursor.execute(sorting_query_by_duration)

print("Top 5 movies by duration:")

print(cursor.fetchall())

sorting_query_by_year='''SELECT title,year FROM movies ORDER BY year DESC LIMIT 5;'''

result=cursor.execute(sorting_query_by_year)

print("Top 5 movies by year:")

print(cursor.fetchall())

sorting_query_by_avg_rating='''SELECT movies.title,AVG(ratings.rating) as avg_rating FROM movies JOIN ratings ON movies.id=ratings.movie_id GROUP BY movies.title ORDER BY avg_rating DESC LIMIT 5;'''

result=cursor.execute(sorting_query_by_avg_rating)

print("Top 5 movies by average rating:")

print(cursor.fetchall())

sorting_query_by_number_of_ratings='''SELECT movies.title,COUNT(ratings.rating) as number_of_ratings FROM movies JOIN ratings ON movies.id=ratings.movie_id GROUP BY movies.title ORDER BY number_of_ratings DESC LIMIT 5;'''

result=cursor.execute(sorting_query_by_number_of_ratings)

print("Top 5 movies by number of ratings:")

print(cursor.fetchall())

         