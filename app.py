import streamlit as st 
import psycopg2
import pandas as pd
import json

# Database connection setup
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        dbname="movie",
        user="postgres",
        password="108",
        host="localhost",
        port="5432"
    )

def fetch_query(query, params=None):
    conn = get_connection()
    cursor = conn.cursor()
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    conn.close()
    return pd.DataFrame(data, columns=columns)

# Streamlit App
st.title("Movie Management Database")

# Sidebar menu
menu = ["Home", "Search Movies", "Movie Summary", "Genre Summary", "User Activity", "Add/Update Movie"]
choice = st.sidebar.selectbox("Menu", menu)

if choice == "Home":
    st.write("Welcome to the Movie Management Database!")
    st.write("This system allows you to explore movies, analyze ratings, and manage the database.")
    st.write("Use the sidebar to navigate through different functionalities.")

elif choice == "Search Movies":
    st.subheader("Search Movies")
    search_option = st.radio("Search by:", ("Title", "Genre"))
    
    if search_option == "Title":
        title = st.text_input("Enter part of the movie title:")
        if title:
            query = """
            SELECT movieId, title, genres 
            FROM movie
            WHERE title ILIKE %s
            """
            result = fetch_query(query, (f"%{title}%",))
            if not result.empty:
                st.dataframe(result)
            else:
                st.write("No movies found.")
    
    elif search_option == "Genre":
        genre = st.text_input("Enter a genre:")
        if genre:
            query = """
            SELECT movieId, title, genres 
            FROM movie
            WHERE genres @> %s::jsonb
            """
            result = fetch_query(query, (json.dumps([genre]),))
            if not result.empty:
                st.dataframe(result)
            else:
                st.write("No movies found in this genre.")

elif choice == "Movie Summary":
    st.subheader("Movie Summary")
    movie_id = st.number_input("Enter Movie ID:", min_value=1, step=1)
    if st.button("Get Summary"):
        query = """
        SELECT m.title, m.genres, 
               AVG(r.rating) as avg_rating, 
               COUNT(DISTINCT r.userId) as num_ratings,
               COUNT(DISTINCT t.tag) as num_tags
        FROM movie m
        LEFT JOIN rating r ON m.movieId = r.movieId
        LEFT JOIN tag t ON m.movieId = t.movieId
        WHERE m.movieId = %s
        GROUP BY m.movieId, m.title, m.genres
        """
        result = fetch_query(query, (movie_id,))
        if not result.empty:
            st.write(result)
            st.write(f"Genres: {result['genres'].iloc[0]}")
        else:
            st.write("Movie not found.")

elif choice == "Genre Summary":
    st.subheader("Genre Summary")
    query = """
    SELECT g.genre, 
           COUNT(DISTINCT m.movieId) as movie_count, 
           AVG(r.rating) as avg_rating
    FROM (SELECT DISTINCT jsonb_array_elements_text(genres) as genre FROM movie) g
    JOIN movie m ON m.genres @> jsonb_build_array(g.genre)
    LEFT JOIN rating r ON m.movieId = r.movieId
    GROUP BY g.genre
    ORDER BY avg_rating DESC
    """
    result = fetch_query(query)
    st.dataframe(result)

elif choice == "User Activity":
    st.subheader("User Activity")
    user_id = st.number_input("Enter User ID:", min_value=1, step=1)
    if st.button("Get Activity"):
        query = """
        SELECT 'Rating' as activity_type, m.title, r.rating, r.timestamp
        FROM rating r
        JOIN movie m ON r.movieId = m.movieId
        WHERE r.userId = %s
        UNION ALL
        SELECT 'Tag' as activity_type, m.title, t.tag, t.timestamp
        FROM tag t
        JOIN movie m ON t.movieId = m.movieId
        WHERE t.userId = %s
        ORDER BY timestamp DESC
        LIMIT 10
        """
        result = fetch_query(query, (user_id, user_id))
        if not result.empty:
            st.dataframe(result)
        else:
            st.write("No activity found for this user.")

elif choice == "Add/Update Movie":
    st.subheader("Add/Update Movie")
    movie_id = st.number_input("Movie ID (leave 0 for new movie):", min_value=0, step=1)
    title = st.text_input("Title:")
    genres = st.multiselect("Genres:", ["Action", "Comedy", "Drama", "Sci-Fi", "Thriller", "Romance", "Adventure"])
    
    if st.button("Submit"):
        genres_json = json.dumps(genres)
        if movie_id == 0:
            query = "INSERT INTO movie (title, genres) VALUES (%s, %s)"
            params = (title, genres_json)
        else:
            query = "UPDATE movie SET title = %s, genres = %s WHERE movieId = %s"
            params = (title, genres_json, movie_id)
        
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            conn.commit()
            st.success("Movie added/updated successfully!")
        except Exception as e:
            conn.rollback()
            st.error(f"An error occurred: {e}")
        finally:
            conn.close()

# Add more functionalities as needed
