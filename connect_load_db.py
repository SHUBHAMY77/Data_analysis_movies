import psycopg2
from psycopg2 import OperationalError

external_url = "postgres://root:GJXyD72jjGt6IfsPb7Ogzq91owv0tGD5@dpg-cliqj7cm411s73dt344g-a.singapore-postgres.render.com/movie_data_analysis"
try:
    # Established a connection to the PostgreSQL database
    connection = psycopg2.connect(external_url)

    # Create a cursor object to interact with the database
    cursor = connection.cursor()
    #
    create_movies=  """
        DROP TABLE IF EXISTS ratings;
        DROP TABLE IF EXISTS movies;
        CREATE TABLE movies (
            id INT PRIMARY KEY,
            title TEXT NOT NULL,
            year INT,
            country TEXT,
            genre TEXT,
            director TEXT,
            minutes INT,
            poster TEXT
        )
        """
    cursor.execute(create_movies)
    create_ratings =    """ 
        CREATE TABLE ratings (
            rater_id INT,
            movie_id INT,
            rating INT,
            time TIMESTAMP,
            FOREIGN KEY (movie_id)
                REFERENCES movies (id)
                ON UPDATE CASCADE ON DELETE CASCADE
        )
        """
    cursor.execute(create_ratings)
  
    grant_acess =  "GRANT pg_read_server_files TO root;"
    cursor.execute(grant_acess)

    insert_data_movies =  "COPY movies FROM '/home/shubham/Desktop/desjtio/Data _Analysis/movies.csv' DELIMITER ',' CSV HEADER;"
    cursor.execute(insert_data_movies)

    insert_data_ratings =  "COPY ratings FROM '/home/shubham/Desktop/desjtio/Data _Analysis/ratings.csv' DELIMITER ',' CSV HEADER;"
    cursor.execute(insert_data_ratings)


except OperationalError as e:
    print(f"Error: {e}")

finally:
    # Close the cursor and connection in the final block
    if cursor:
        cursor.close()
    if connection:
        connection.close()


