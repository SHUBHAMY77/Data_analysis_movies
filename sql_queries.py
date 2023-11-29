import psycopg2
from psycopg2 import OperationalError

external_url = "postgres://root:GJXyD72jjGt6IfsPb7Ogzq91owv0tGD5@dpg-cliqj7cm411s73dt344g-a.singapore-postgres.render.com/movie_data_analysis"

try:
    # Established a connection to the PostgreSQL database
    connection = psycopg2.connect(external_url)

    # Create a cursor object to interact with the database
    cursor = connection.cursor()

    # Query to get the top 5 movies based on duration
    duration_query = "SELECT title, minutes FROM movies ORDER BY minutes DESC LIMIT 5;"
    cursor.execute(duration_query)
    top_movies_duration = cursor.fetchall()

        # Print the results
    print("Top 5 Movies Based on Duration:")
    for row in top_movies_duration:
        print(row)

    # Query to get the top 5 movies based on year of release
    year_query = "SELECT title, year FROM movies ORDER BY year DESC LIMIT 5;"
    cursor.execute(year_query)
    top_movies_year = cursor.fetchall()

    # Print the results

    print("\nTop 5 Movies Based on Year of Release:")
    for row in top_movies_year:
        print(row)


    # Query to get the top 5 movies based on average rating (considering movies with minimum 5 ratings)
    avg_rating_query = """
        SELECT m.title, AVG(r.rating) as avg_rating
        FROM movies m
        JOIN ratings r ON m.movie_id = r.movie_id
        GROUP BY m.title
        HAVING COUNT(r.rating) >= 5
        ORDER BY avg_rating DESC
        LIMIT 5;
    """
    cursor.execute(avg_rating_query)
    top_movies_avg_rating = cursor.fetchall()


    # Print the results
    print("\nTop 5 Movies Based on Average Rating:")
    for row in top_movies_avg_rating:
        print(row)

    # Query to get the top 5 movies based on the number of ratings given
    num_ratings_query = """
        SELECT m.title, COUNT(r.rating) as num_ratings
        FROM movies m
        JOIN ratings r ON m.movie_id = r.movie_id
        GROUP BY m.title
        ORDER BY num_ratings DESC
        LIMIT 5;
    """
    cursor.execute(num_ratings_query)
    top_movies_num_ratings = cursor.fetchall()



    print("\nTop 5 Movies Based on Number of Ratings:")
    for row in top_movies_num_ratings:
        print(row)

    # Query to get the number of unique rater IDs
    unique_raters_query = "SELECT COUNT(DISTINCT rater_id) FROM ratings;"
    cursor.execute(unique_raters_query)
    unique_raters_count = cursor.fetchone()[0]

    print("\nNumber of Unique Raters:")
    print(unique_raters_count)

    # Query to get the top 5 rater IDs based on most movies rated
    top_rater_movies_query = """
        SELECT rater_id, COUNT(movie_id) as num_movies
        FROM ratings
        GROUP BY rater_id
        ORDER BY num_movies DESC
        LIMIT 5;
    """
    cursor.execute(top_rater_movies_query)
    top_rater_movies = cursor.fetchall()

    print("\nTop 5 Rater IDs Based on Most Movies Rated:")
    for row in top_rater_movies:
        print(row)

    # Query to get the top 5 rater IDs based on highest average rating given (consider raters with min 5 ratings)
    top_rater_avg_rating_query = """
        SELECT rater_id, AVG(rating) as avg_rating
        FROM ratings
        GROUP BY rater_id
        HAVING COUNT(rating) >= 5
        ORDER BY avg_rating DESC
        LIMIT 5;
    """
    cursor.execute(top_rater_avg_rating_query)
    top_rater_avg_rating = cursor.fetchall()

    print("\nTop 5 Rater IDs Based on Highest Average Rating Given:")
    for row in top_rater_avg_rating:
        print(row)

        # e. Favorite Movie Genre of Rater ID 1040
    favorite_genre_query = """
           SELECT genre, COUNT(*) as num_ratings
           FROM movies m
           JOIN ratings r ON m.movie_id = r.movie_id
           WHERE rater_id = 1040
           GROUP BY genre
           ORDER BY num_ratings DESC
           LIMIT 1;
       """
    cursor.execute(favorite_genre_query)
    favorite_genre = cursor.fetchone()

    print("\nFavorite Movie Genre of Rater ID 1040:")
    print(favorite_genre)

    # f. Highest Average Rating for a Movie Genre by Rater ID 1040
    highest_avg_rating_query = """
           SELECT genre, AVG(rating) as avg_rating
           FROM movies m
           JOIN ratings r ON m.movie_id = r.movie_id
           WHERE rater_id = 1040
           GROUP BY genre
           HAVING COUNT(rating) >= 5
           ORDER BY avg_rating DESC
           LIMIT 1;
       """
    cursor.execute(highest_avg_rating_query)
    highest_avg_rating = cursor.fetchone()

    print("\nHighest Average Rating for a Movie Genre by Rater ID 1040:")
    print(highest_avg_rating)

 

except OperationalError as e:
    print(f"Error: {e}")

finally:
    # Close the cursor and connection in the final block
    if cursor:
        cursor.close()
    if connection:
        connection.close()