import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
from io import BytesIO
from PIL import Image
import ast
import time
import os

API_KEY = 'e86667b97885b7680474d5a2bbe4ff05'  # Your TMDb API key here
DATASET_PATH = r'C:\Users\skand\Downloads\archive\TMDB_movie_dataset_v11.csv'  # Update path
PARQUET_PATH = DATASET_PATH.replace('.csv', '.parquet')  # Optional: parquet path

IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w185"

# Language code to full language name mapping
lang_code_map = {
    'en': 'English',
    'ta': 'Tamil',
    'fr': 'French',
    'es': 'Spanish',
    'de': 'German',
    'hi': 'Hindi',
    'zh': 'Chinese',
    'ja': 'Japanese',
    'ru': 'Russian',
    'it': 'Italian',
    'ko': 'Korean',
}

@st.cache_data(show_spinner=False)
def get_genre_map(api_key):
    url = f"https://api.themoviedb.org/3/genre/movie/list?api_key={api_key}&language=en-US"
    resp = requests.get(url)
    resp.raise_for_status()
    genres = resp.json().get('genres', [])
    return {genre['id']: genre['name'] for genre in genres}

def get_genre_names(genre_ids, genre_map):
    if isinstance(genre_ids, str):
        try:
            genre_list = ast.literal_eval(genre_ids)
            if isinstance(genre_list, list) and len(genre_list) > 0 and isinstance(genre_list[0], dict):
                genre_ids = [g.get('id') for g in genre_list]
            elif isinstance(genre_list, list):
                genre_ids = genre_list
            else:
                genre_ids = []
        except Exception:
            genre_ids = []
    if not isinstance(genre_ids, list):
        return ""
    names = [genre_map.get(gid, "") for gid in genre_ids]
    return ', '.join(filter(None, names))

def format_runtime(runtime_min):
    if pd.isna(runtime_min) or runtime_min == 0:
        return "N/A"
    hours = int(runtime_min) // 60
    mins = int(runtime_min) % 60
    if hours > 0:
        return f"{hours}h {mins}m"
    else:
        return f"{mins}m"

@st.cache_data(show_spinner=False)
def load_data(csv_path=DATASET_PATH, parquet_path=PARQUET_PATH):
    """
    Load the movie dataset, try Parquet first (much faster), fallback to CSV.
    Parse release_date column, filter out invalid dates.
    Parse genres to list of genre ids.
    """
    if os.path.exists(parquet_path):
        df = pd.read_parquet(parquet_path)
    else:
        # Select only required columns to speed up CSV loading (adjust cols if needed)
        usecols = [
            'id', 'title', 'genres', 'release_date', 'runtime',
            'vote_average', 'original_language', 'overview', 'poster_path'
        ]
        df = pd.read_csv(csv_path, usecols=usecols)
        # Save Parquet cache for next time - do it only once
        df.to_parquet(parquet_path)

    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    df = df[df['release_date'].notna()]

    # Parse genres column to list of ids
    def parse_genres(cell):
        if pd.isna(cell):
            return []
        try:
            lst = ast.literal_eval(cell)
            if isinstance(lst, list) and all(isinstance(i, dict) and 'id' in i for i in lst):
                return [i['id'] for i in lst]
        except Exception:
            pass
        return []

    if 'genres' in df.columns:
        df['genre_ids'] = df['genres'].apply(parse_genres)
    else:
        df['genre_ids'] = [[] for _ in range(len(df))]

    return df

def find_movies_with_fallback(df, max_days_back=30):
    now = datetime.now()
    for delta_days in range(max_days_back):
        check_date = now - timedelta(days=delta_days)
        m, d = check_date.month, check_date.day
        filtered = df[(df['release_date'].dt.month == m) & (df['release_date'].dt.day == d)]
        filtered = filtered[filtered['release_date'].dt.date <= now.date()]
        if not filtered.empty:
            return filtered, m, d
    return pd.DataFrame(), None, None

def get_poster_image(poster_path, load_images):
    """
    Fetch poster image only if load_images is True.
    """
    if not load_images:
        return None
    if poster_path and isinstance(poster_path, str) and poster_path.startswith('/'):
        poster_url = IMAGE_BASE_URL + poster_path
        try:
            resp = requests.get(poster_url, timeout=5)
            resp.raise_for_status()
            img = Image.open(BytesIO(resp.content))
            return img
        except Exception:
            pass
    return None

def display_movie(movie, genre_map, is_fav, load_images=True):
    poster_img = get_poster_image(movie['poster_path'], load_images)
    if poster_img:
        st.image(poster_img, width=100)
    else:
        st.write("No image available")
    st.markdown(f"**{movie['title']}**")
    st.markdown(f"Release Date: {movie['release_date'].strftime('%Y-%m-%d')}")
    st.markdown(f"Runtime: {format_runtime(movie['runtime'])}")
    st.markdown(f"Rating: {movie['vote_average']}")
    genres = get_genre_names(movie.get('genres') or movie.get('genre_ids'), genre_map)
    st.markdown(f"Genres: {genres}")
    lang = lang_code_map.get(movie['original_language'], movie['original_language'])
    st.markdown(f"Language: {lang}")
    if movie['overview']:
        st.markdown(f"Overview: {movie['overview']}")
    fav_text = "Remove from Favorites" if is_fav else "Add to Favorites"
    return st.button(fav_text, key=f"fav_{movie['id']}")

def main():
    st.title("Today in Movie History - Movie Explorer")
    st.sidebar.header("Settings")

    # Option to skip poster images for faster loading
    load_images = st.sidebar.checkbox("Load Poster Images (Slower)", value=True, help="Uncheck to speed up loading")

    if not API_KEY or API_KEY == 'your_api_key_here':
        st.error("Please set a valid TMDb API key in the script.")
        return

    try:
        st.info("Loading genres from TMDb API...")
        start = time.time()
        genre_map = get_genre_map(API_KEY)
        st.success(f"Loaded {len(genre_map)} genres in {time.time() - start:.2f} seconds")
        if not genre_map:
            st.warning("No genres were loaded from the API.")
    except Exception as e:
        st.error(f"Failed to load genres from TMDb API: {e}")
        return

    try:
        st.info("Loading movie dataset...")
        start = time.time()
        df = load_data()
        st.success(f"Loaded {df.shape[0]} movies in {time.time() - start:.2f} seconds")
        if df.empty:
            st.warning("Dataset is empty after loading.")
            return
    except Exception as e:
        st.error(f"Error loading dataset: {e}")
        return

    # Find movies for today or fallback date
    filtered_df, used_month, used_day = find_movies_with_fallback(df)
    if filtered_df.empty:
        st.warning("No movies found for today or fallback dates in last 30 days. Showing all movies.")
        filtered_df = df

    if used_month and used_day:
        st.write(f"Showing movies released on {used_month:02d}-{used_day:02d}")
    else:
        st.write(f"Showing all available movies")

    # Sidebar filters
    st.sidebar.header("Filters")

    genres_list = sorted(set(genre_map.values()))
    selected_genre = st.sidebar.selectbox("Genre", options=[""] + genres_list)

    langs_codes = sorted(df['original_language'].dropna().unique())
    langs_list = sorted({lang_code_map.get(code, code) for code in langs_codes})
    selected_language = st.sidebar.selectbox("Language", options=[""] + langs_list)

    min_rating = st.sidebar.slider("Minimum Rating", 0.0, 10.0, 0.0, step=0.1)

    movies_to_show = filtered_df.copy()

    if selected_genre:
        movies_to_show = movies_to_show[movies_to_show['genres'].apply(
            lambda s: selected_genre in get_genre_names(s, genre_map))]

    if selected_language:
        code_filter = None
        for k, v in lang_code_map.items():
            if v == selected_language:
                code_filter = k
                break
        if code_filter:
            movies_to_show = movies_to_show[movies_to_show['original_language'] == code_filter]
        else:
            movies_to_show = movies_to_show[movies_to_show['original_language'] == selected_language]

    if min_rating > 0:
        movies_to_show = movies_to_show[movies_to_show['vote_average'] >= min_rating]

    if movies_to_show.empty:
        st.warning("No movies found for the selected filters.")
        return

    # Initialize session state if not present
    if 'page' not in st.session_state:
        st.session_state.page = 0
    if 'favorites' not in st.session_state:
        st.session_state.favorites = set()

    total_movies = len(movies_to_show)
    page_size = 10
    max_page = max(0, (total_movies - 1) // page_size)

    start_idx = st.session_state.page * page_size
    end_idx = start_idx + page_size
    page_movies = movies_to_show.iloc[start_idx:end_idx]

    st.write(f"Showing movies {start_idx + 1} to {min(end_idx, total_movies)} of {total_movies}")

    for _, movie in page_movies.iterrows():
        is_fav = movie['id'] in st.session_state.favorites
        clicked = display_movie(movie, genre_map, is_fav, load_images=load_images)
        if clicked:
            movie_id = movie['id']
            if is_fav:
                st.session_state.favorites.remove(movie_id)
            else:
                st.session_state.favorites.add(movie_id)
            st.experimental_rerun()

    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        if st.button("Previous") and st.session_state.page > 0:
            st.session_state.page -= 1
            st.experimental_rerun()
    with col3:
        if st.button("Next") and st.session_state.page < max_page:
            st.session_state.page += 1
            st.experimental_rerun()

    st.sidebar.subheader("Favorites")
    st.sidebar.write(f"Favorites count: {len(st.session_state.favorites)}")
    if st.sidebar.button("Show Favorites"):
        fav_df = df[df['id'].isin(st.session_state.favorites)]
        if fav_df.empty:
            st.sidebar.info("No favorite movies selected.")
        else:
            st.sidebar.write("Favorite Movies:")
            for _, movie in fav_df.iterrows():
                year = movie['release_date'].year if not pd.isna(movie['release_date']) else 'N/A'
                st.sidebar.markdown(f"- {movie['title']} ({year})")

    if st.sidebar.button("Clear Cache"):
        st.cache_data.clear()
        st.experimental_rerun()


if __name__ == "__main__":
    main()
