import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
from io import BytesIO
from PIL import Image
import ast
import os
import gdown
import time

# ======= CONFIG =======
API_KEY = 'e86667b97885b7680474d5a2bbe4ff05'  # Your TMDb API key here
GOOGLE_DRIVE_FILE_ID = "186iL_Oy0lFX_3QzzDCvdAdldgjlFPBm4"  # Your Google Drive Parquet file ID
CACHED_PARQUET_PATH = "cached_movie_dataset.parquet"
IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w185"

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

# ======= HELPER FUNCTIONS =======

def safe_rerun():
    """Call st.rerun() if available."""
    # st.rerun() is the current recommended way to rerun the app
    # st.experimental_rerun() is deprecated.
    st.rerun()

@st.cache_data(show_spinner=False)
def get_genre_map(api_key, retries=3, delay=2):
    url = f"https://api.themoviedb.org/3/genre/movie/list?api_key={api_key}&language=en-US"
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            genres = resp.json().get('genres', [])
            return {genre['id']: genre['name'] for genre in genres}
        except requests.exceptions.RequestException as e:
            st.warning(f"TMDb API request failed (attempt {attempt}/{retries}). Retrying in {delay} seconds...")
            time.sleep(delay)
    st.error(f"Failed to fetch genres from TMDb API after {retries} attempts. Check your network or API key.")
    return {}

@st.cache_data(show_spinner=False)
def load_data():
    if not os.path.exists(CACHED_PARQUET_PATH):
        download_url = f"https://drive.google.com/uc?id={GOOGLE_DRIVE_FILE_ID}"
        st.info("Downloading dataset from Google Drive, please wait...")
        gdown.download(download_url, CACHED_PARQUET_PATH, quiet=False)

    df = pd.read_parquet(CACHED_PARQUET_PATH)
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    df = df[df['release_date'].notna()]

    if 'genres' in df.columns:
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

        df['genre_ids'] = df['genres'].apply(parse_genres)
    else:
        df['genre_ids'] = [[] for _ in range(len(df))]
    return df

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

def get_poster_image(poster_path):
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

def display_movie(movie, genre_map, is_fav):
    poster_img = get_poster_image(movie['poster_path'])
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

# ======= MAIN APP =======

def main():
    st.title("Today in Movie History - Movie Explorer")

    genre_map = get_genre_map(API_KEY)
    if not genre_map:
        st.error("Cannot load genre data from TMDb. Please try again later.")
        return

    df = load_data()

    filtered_df, used_month, used_day = find_movies_with_fallback(df)
    if filtered_df.empty:
        filtered_df = df
        used_month, used_day = None, None

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

    st.write(f"Showing movies {start_idx+1} to {min(end_idx, total_movies)} of {total_movies}")

    for _, movie in page_movies.iterrows():
        is_fav = movie['id'] in st.session_state.favorites
        clicked = display_movie(movie, genre_map, is_fav)
        if clicked:
            movie_id = movie['id']
            if is_fav:
                st.session_state.favorites.remove(movie_id)
            else:
                st.session_state.favorites.add(movie_id)
            safe_rerun()

    col1, _, col3 = st.columns([1, 2, 1])
    with col1:
        if st.button("Previous") and st.session_state.page > 0:
            st.session_state.page -= 1
            safe_rerun()
    with col3:
        if st.button("Next") and st.session_state.page < max_page:
            st.session_state.page += 1
            safe_rerun()

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
        safe_rerun()

if __name__ == "__main__":
    main()
