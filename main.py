import psycopg2
import csv
from datetime import datetime
import pandas as pd

class MovieRatingsDatabase:
    def __init__(self, database="movies_ratings", user='postgres', password='1234', host='127.0.0.1', port='5432'):
        # Connecting to the database and using a local database because render doesn't have superuser rights for importing csv
        self.conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def create_tables(self):
        # Function to create both tables
        creation_query_movies = '''
        CREATE TABLE MOVIES(
            id int NOT NULL,
            title varchar, 
            year int,
            country varchar,
            genre varchar,
            director varchar,
            minutes int,
            poster varchar
        );
        '''

        creation_query_ratings = '''
        CREATE TABLE RATINGS(
            rater_id int NOT NULL,
            movie_id int NOT NULL,
            rating int,
            time timestamp
        );
        '''

        self.cursor.execute(creation_query_movies)
        self.cursor.execute(creation_query_ratings)
        print("Tables created successfully........")

    def insert_data(self, movies_csv_path='E:/Job_Assignments/squadcast-assessment/movies.csv', ratings_csv_path='ratings.csv'):
        # Function to insert data from csv to db
        insertion_query_movies = f'''COPY movies FROM '{movies_csv_path}' DELIMITER ',' CSV HEADER;'''
        self.cursor.execute(insertion_query_movies)

        with open(ratings_csv_path, 'r') as ratings_csv:
            csv_reader = csv.reader(ratings_csv)

            for row in csv_reader:
                if row[0] == 'rater_id':
                    continue
                timestamp = datetime.utcfromtimestamp(int(row[3]))
                self.cursor.execute("INSERT INTO RATINGS (rater_id, movie_id, rating, time) VALUES (%s, %s, %s, %s)",
                                    (row[0], row[1], row[2], timestamp))

        print("Data inserted successfully........")

    def sorting_query(self, method='avg_rating'):
        # Function to perform sorting queries
        if method == 'duration':
            sorting_query = '''SELECT title, minutes FROM movies ORDER BY minutes DESC LIMIT 5;'''
        elif method == 'year':
            sorting_query = '''SELECT title, year FROM movies ORDER BY year DESC LIMIT 5;'''
        elif method == 'number_of_ratings':
            # We use inner join to get the number of ratings for each movie
            sorting_query = '''SELECT movies.title, COUNT(ratings.rating) as number_of_ratings
                                FROM movies
                                JOIN ratings ON movies.id = ratings.movie_id
                                GROUP BY movies.title
                                ORDER BY number_of_ratings DESC LIMIT 5;'''
        else:  # Default is 'avg_rating'
            # Here also we use inner join to get the average rating for each movie
            sorting_query = '''SELECT movies.title, AVG(ratings.rating) as avg_rating
                                FROM movies
                                JOIN ratings ON movies.id = ratings.movie_id
                                GROUP BY movies.title
                                HAVING COUNT(ratings.rating) >= 5
                                ORDER BY avg_rating DESC LIMIT 5;'''

        self.cursor.execute(sorting_query)
        result = self.cursor.fetchall()

        if method == 'duration':
            print("Top 5 movies by duration:")
        elif method == 'year':
            print("Top 5 movies by year:")
        elif method == 'number_of_ratings':
            print("Top 5 movies by number of ratings:")
        else:  # Default is 'avg_rating'
            print("Top 5 movies by average rating:")

        # Convert the result to a Pandas DataFrame for better representation
        df = pd.DataFrame(result, columns=['Title', method.capitalize()])
        print(df,"\n")

    def unique_rating_ids(self):
        self.cursor.execute("SELECT DISTINCT rater_id FROM ratings;")
        result = self.cursor.fetchall()
        print("Unique rating ids: ", result[0][0],"\n")


    def sort_rater_ids_by_most_movies_rated(self):
        self.cursor.execute("SELECT rater_id, COUNT(movie_id) as movies_rated_count FROM ratings GROUP BY rater_id ORDER BY movies_rated_count DESC LIMIT 5;")
        result = self.cursor.fetchall()

        # Convert the result to a Pandas DataFrame for better representation
        df = pd.DataFrame(result, columns=['Rater ID', 'Movies Rated Count'])
        print("Top 5 rater ids by most movies rated: \n")
        print(df,"\n")
    
    def sort_rater_ids_by_max_avg_ratings(self):
        self.cursor.execute("""SELECT rater_id, AVG(rating) AS avg_rating
                                FROM ratings
                                GROUP BY rater_id
                                HAVING COUNT(rating) >= 5
                                ORDER BY avg_rating DESC
                                LIMIT 5;
                                """)
        result = self.cursor.fetchall()
        
        # Convert the result to a Pandas DataFrame for better representation
        df = pd.DataFrame(result, columns=['Rater ID', 'Average Rating'])
        print("Top 5 rater ids by max average ratings: \n")
        print(df,"\n")
        
    
    def top_rated_by_michael_bay(self):
        self.cursor.execute("""SELECT movies.title, AVG(ratings.rating) AS avg_rating
                                FROM movies
                                JOIN ratings ON movies.id = ratings.movie_id
                                WHERE movies.director = 'Michael Bay'
                                GROUP BY movies.title
                                ORDER BY avg_rating DESC
                                LIMIT 5;
                                """)
        result = self.cursor.fetchall()
        
        #Convert the result to a Pandas DataFrame for better representation
        df = pd.DataFrame(result, columns=['Title', 'Average Rating'])
        print("Top 5 movies directed by Michael Bay: \n")
        print(df,"\n")
    
    def top_rated_by_comedy(self):
        self.cursor.execute("""SELECT movies.title, AVG(ratings.rating) AS avg_rating
                                FROM movies
                                JOIN ratings ON movies.id = ratings.movie_id
                                WHERE movies.genre = 'Comedy'
                                GROUP BY movies.title
                                ORDER BY avg_rating DESC
                                LIMIT 5;
                                """)
        result=self.cursor.fetchall()
        
        
        # Convert the result to a Pandas DataFrame for better representation
        df = pd.DataFrame(result, columns=['Title', 'Average Rating'])
        print("Top 5 comedy movies: \n")
        print(df,"\n")
    
    def top_rated_in_2013(self):
        self.cursor.execute("""SELECT movies.title, AVG(ratings.rating) AS avg_rating
                                FROM movies
                                JOIN ratings ON movies.id = ratings.movie_id
                                WHERE movies.year = 2013
                                GROUP BY movies.title
                                ORDER BY avg_rating DESC
                                LIMIT 5;
                                """)
        result=self.cursor.fetchall()
        
        # Convert the result to a Pandas DataFrame for better representation
        df = pd.DataFrame(result, columns=['Title', 'Average Rating'])
        print("Top 5 movies rated in 2013: \n")
        print(df,"\n")
    
    def top_rated_in_india(self):
        self.cursor.execute("""SELECT movies.title, AVG(ratings.rating) AS avg_rating
                                FROM movies
                                JOIN ratings ON movies.id = ratings.movie_id
                                WHERE movies.country = 'India'
                                GROUP BY movies.title
                                HAVING COUNT(ratings.rating) >= 5
                                ORDER BY avg_rating DESC
                                LIMIT 5;
                                """)
        result=self.cursor.fetchall()
        
        # Convert the result to a Pandas DataFrame for better representation
        df = pd.DataFrame(result, columns=['Title', 'Average Rating'])
        print("Top 5 movies rated in India: \n")
        print(df,"\n")
    
    def fav_movie_genre_1040(self):
        # We first fetch all the ratings for rater_id 1040
        self.cursor.execute("""SELECT movies.genre
                                FROM movies
                                JOIN ratings ON movies.id = ratings.movie_id
                                WHERE ratings.rater_id = 1040
                                GROUP BY movies.genre
                                ORDER BY COUNT(ratings.rating) DESC
                                """)
        result=self.cursor.fetchall()
        
        #We can also put limit on the upper query as 1 to get the exact genre which will be 'Action, Adventure, Sci-Fi' but I thought a single genre would be better :)
        
        
        # We then create a map of all the genres separated by comma and their count
        
        genre_map = {}
        
        for genre_string in result:
            for genre in genre_string[0].split(','):
                if genre in genre_map:
                    genre_map[genre] += 1
                else:
                    genre_map[genre] = 1
        
        sorted_map = sorted(genre_map.items(), key=lambda x: x[1], reverse=True)
        
        print("Favourite movie genre for rater_id 1040: ", sorted_map[0][0],"\n")
    
    def highest_avg_rating_genre_by_1040(self):
        self.cursor.execute("""SELECT movies.genre, AVG(ratings.rating) AS avg_rating
                                FROM movies
                                JOIN ratings ON movies.id = ratings.movie_id
                                WHERE ratings.rater_id = 1040
                                GROUP BY movies.genre
                                HAVING COUNT(ratings.rating) >= 5
                                ORDER BY avg_rating DESC;
                                """)
        result=self.cursor.fetchall()
        
        # We can again put limit on the upper query as 1 to get the exact genre which will be 'Action, Adventure, Sci-Fi' but I thought a single genre would be better :)

        # We then create a map of all the genres separated by comma and their count
        
        genre_map = {}
        
        for genre_string in result:
            for genre in genre_string[0].split(','):
                if genre in genre_map:
                    genre_map[genre] += 1
                else:
                    genre_map[genre] = 1
        
        sorted_map = sorted(genre_map.items(), key=lambda x: x[1], reverse=True)
        
        
        print("Highest Average Rating for a movie genre for rater_id 1040: ", sorted_map[0][0],"\n")
        
        
    def year_with_second_highest_action_movies(self):
        # We use CTE to get the year with second highest action movies 
        self.cursor.execute("""WITH RankedYears AS (
                                SELECT
                                    movies.year,
                                    ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS row_num
                                FROM
                                    movies
                                    JOIN ratings ON movies.id = ratings.movie_id
                                WHERE
                                    LOWER(movies.genre) LIKE '%action%'
                                    AND movies.country = 'USA'
                                    AND movies.minutes < 120
                                GROUP BY
                                    movies.year
                                HAVING
                                    AVG(ratings.rating) >= 6.5
                            )
                            SELECT
                                year
                            FROM
                                RankedYears
                            WHERE
                                row_num = 2;
                            """)
        
        result=self.cursor.fetchall()
        
        print("Year with second highest action movies: ", result[0][0],"\n")
        
    def count_of_movies_with_high_ratings(self):
        
        self.cursor.execute("""SELECT COUNT(movie_id) AS high_rated_movies_count
                                FROM ratings
                                WHERE rating >= 7
                                GROUP BY movie_id
                                HAVING COUNT(movie_id) >= 5;
                                """)

        result=self.cursor.fetchall()
        
        print("Count of movies with high ratings: ", result[0][0],"\n")
    
    

movie_ratings_db = MovieRatingsDatabase()

#Creating tables

movie_ratings_db.create_tables()

#Inserting data from csv to db

movie_ratings_db.insert_data()


# Sorting Queries for all the methods in task 2-a
movie_ratings_db.sorting_query('duration')
movie_ratings_db.sorting_query('year')
movie_ratings_db.sorting_query('number_of_ratings')
movie_ratings_db.sorting_query('avg_rating')

# Printing unique rating ids for task 2-b
movie_ratings_db.unique_rating_ids()

# Sorting rater IDs by most movies rated and max avg rating for task 2-c
movie_ratings_db.sort_rater_ids_by_most_movies_rated()
movie_ratings_db.sort_rater_ids_by_max_avg_ratings()

#Top 5 rated movies by different parameters for task 2-d
movie_ratings_db.top_rated_by_michael_bay()
movie_ratings_db.top_rated_by_comedy()
movie_ratings_db.top_rated_in_2013()
movie_ratings_db.top_rated_in_india()

#Top rated movie genre for rater_id 1040 for task 2-e
movie_ratings_db.fav_movie_genre_1040()

#Highest average rating genre for rater_id 1040 for task 2-f
movie_ratings_db.highest_avg_rating_genre_by_1040()

#Year with second highest action movies for task 2-g
movie_ratings_db.year_with_second_highest_action_movies()

#Count of movies with high ratings for task 2-h
movie_ratings_db.count_of_movies_with_high_ratings()