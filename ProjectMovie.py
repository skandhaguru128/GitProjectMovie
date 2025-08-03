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
# IMPORTANT: Replace 'YOUR_TMDB_API_KEY' with your actual TMDb API key
API_KEY = 'e86667b97885b7680474d5a2bbe4ff05' # Your TMDb API key here
# IMPORTANT: Replace 'YOUR_GOOGLE_DRIVE_FILE_ID' with your actual Google Drive Parquet file ID
GOOGLE_DRIVE_FILE_ID = "186iL_Oy0lFX_3QzzDCvdAdldgjlFPBm4"  # Your Google Drive Parquet file ID
CACHED_PARQUET_PATH = "cached_movie_dataset.parquet"
IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w185"

# Comprehensive language code map
lang_code_map = {
    'en': 'English', 'ta': 'Tamil', 'fr': 'French', 'es': 'Spanish', 'de': 'German',
    'hi': 'Hindi', 'zh': 'Chinese', 'ja': 'Japanese', 'ru': 'Russian', 'it': 'Italian',
    'ko': 'Korean', 'aa': 'Afar', 'ab': 'Abkhazian', 'ae': 'Avestan', 'af': 'Afrikaans',
    'ak': 'Akan', 'am': 'Amharic', 'an': 'Aragonese', 'ar': 'Arabic', 'as': 'Assamese',
    'av': 'Avaric', 'ay': 'Aymara', 'az': 'Azerbaijani', 'ba': 'Bashkir', 'be': 'Belarusian',
    'bg': 'Bulgarian', 'bh': 'Bihari', 'bi': 'Bislama', 'bm': 'Bambara', 'bn': 'Bengali',
    'bo': 'Tibetan', 'br': 'Breton', 'bs': 'Bosnian', 'ca': 'Catalan', 'ce': 'Chechen',
    'ch': 'Chamorro', 'co': 'Corsican', 'cr': 'Cree', 'cs': 'Czech', 'cu': 'Church Slavic',
    'cv': 'Chuvash', 'cy': 'Welsh', 'da': 'Danish', 'de': 'German', 'dv': 'Divehi',
    'dz': 'Dzongkha', 'ee': 'Ewe', 'el': 'Greek', 'eo': 'Esperanto', 'et': 'Estonian',
    'eu': 'Basque', 'fa': 'Persian', 'ff': 'Fulah', 'fi': 'Finnish', 'fj': 'Fijian',
    'fo': 'Faroese', 'fr': 'French', 'fy': 'Western Frisian', 'ga': 'Irish', 'gd': 'Gaelic',
    'gl': 'Galician', 'gn': 'Guarani', 'gu': 'Gujarati', 'gv': 'Manx', 'ha': 'Hausa',
    'he': 'Hebrew', 'hi': 'Hindi', 'ho': 'Hiri Motu', 'hr': 'Croatian', 'ht': 'Haitian',
    'hu': 'Hungarian', 'hy': 'Armenian', 'hz': 'Herero', 'ia': 'Interlingua (International Auxiliary Language Association)',
    'id': 'Indonesian', 'ie': 'Interlingue', 'ig': 'Igbo', 'ii': 'Sichuan Yi', 'ik': 'Inupiaq',
    'io': 'Ido', 'is': 'Icelandic', 'it': 'Italian', 'iu': 'Inuktitut', 'ja': 'Japanese',
    'jv': 'Javanese', 'ka': 'Georgian', 'kg': 'Kongo', 'ki': 'Kikuyu', 'kj': 'Kuanyama',
    'kk': 'Kazakh', 'kl': 'Kalaallisut', 'km': 'Central Khmer', 'kn': 'Kannada', 'ko': 'Korean',
    'kr': 'Kanuri', 'ks': 'Kashmiri', 'ku': 'Kurdish', 'kv': 'Komi', 'kw': 'Cornish',
    'ky': 'Kirghiz', 'la': 'Latin', 'lb': 'Luxembourgish', 'lg': 'Ganda', 'li': 'Limburgan',
    'ln': 'Lingala', 'lo': 'Lao', 'lt': 'Lithuanian', 'lv': 'Latvian', 'mg': 'Malagasy',
    'mh': 'Marshallese', 'mi': 'Maori', 'mk': 'Macedonian', 'ml': 'Malayalam', 'mn': 'Mongolian',
    'mo': 'Moldavian', # Usually 'ro' for Romanian
    'mr': 'Marathi', 'ms': 'Malay', 'mt': 'Maltese', 'my': 'Burmese', 'na': 'Nauru',
    'nb': 'Norwegian Bokmål', 'nd': 'Ndebele, North', 'ne': 'Nepali', 'ng': 'Ndonga',
    'nl': 'Dutch', 'nn': 'Norwegian Nynorsk', 'no': 'Norwegian', 'nr': 'Ndebele, South',
    'nv': 'Navajo', 'ny': 'Chichewa', 'oc': 'Occitan', 'oj': 'Ojibwa', 'om': 'Oromo',
    'or': 'Oriya', 'os': 'Ossetian', 'pa': 'Panjabi', 'pi': 'Pali', 'pl': 'Polish',
    'ps': 'Pushto', 'pt': 'Portuguese', 'qu': 'Quechua', 'rm': 'Romansh', 'rn': 'Rundi',
    'ro': 'Romanian', 'ru': 'Russian', 'rw': 'Kinyarwanda', 'sa': 'Sanskrit', 'sc': 'Sardinian',
    'sd': 'Sindhi', 'se': 'Northern Sami', 'sg': 'Sango', 'sh': 'Serbo-Croatian', # Deprecated, prefer hr/sr/bs
    'si': 'Sinhala', 'sk': 'Slovak', 'sl': 'Slovenian', 'sm': 'Samoan', 'sn': 'Shona',
    'so': 'Somali', 'sq': 'Albanian', 'sr': 'Serbian', 'ss': 'Swati', 'st': 'Sotho, Southern',
    'su': 'Sundanese', 'sv': 'Swedish', 'sw': 'Swahili', 'te': 'Telugu', 'tg': 'Tajik',
    'th': 'Thai', 'ti': 'Tigrinya', 'tk': 'Turkmen', 'tl': 'Tagalog', 'tn': 'Tswana',
    'to': 'Tonga (Tonga Islands)', 'tr': 'Turkish', 'ts': 'Tsonga', 'tt': 'Tatar',
    'tw': 'Twi', 'ty': 'Tahitian', 'ug': 'Uighur', 'uk': 'Ukrainian', 'ur': 'Urdu',
    'uz': 'Uzbek', 've': 'Venda', 'vi': 'Vietnamese', 'vo': 'Volapük', 'wa': 'Walloon',
    'wo': 'Wolof', 'xh': 'Xhosa', 'yi': 'Yiddish', 'yo': 'Yoruba', 'za': 'Zhuang',
    'zu': 'Zulu', 'xx': 'No Language', 'cn': 'Cantonese (Simplified Chinese)'
}


# ======= HELPER FUNCTIONS =======

def safe_rerun():
    """Call st.rerun() if available."""
    st.rerun()

# Callback function to reset page to 0
def reset_page():
    st.session_state.page = 0

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
def load_data(genre_map): # Now accepts genre_map
    if not os.path.exists(CACHED_PARQUET_PATH):
        download_url = f"https://drive.google.com/uc?id={GOOGLE_DRIVE_FILE_ID}"
        st.info("Downloading dataset from Google Drive, please wait...")
        gdown.download(download_url, CACHED_PARQUET_PATH, quiet=False)

    df = pd.read_parquet(CACHED_PARQUET_PATH)
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    df = df[df['release_date'].notna()]

    # Ensure 'adult' column exists and is boolean
    if 'adult' not in df.columns:
        st.warning("The 'adult' column was not found in your dataset. Adult content filtering will not be effective based on this column.")
        df['adult'] = False # Default to False if column is missing
    else:
        df['adult'] = df['adult'].astype(bool) # Ensure it's boolean

    # Ensure 'popularity' column exists and is numeric (even if not used for sorting)
    if 'popularity' not in df.columns:
        st.warning("The 'popularity' column was not found in your dataset. Popularity data will be unavailable.")
        df['popularity'] = 0.0 # Default to 0.0 if column is missing
    else:
        df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce').fillna(0.0)

    # Process 'keywords' column for searching
    if 'keywords' not in df.columns:
        st.warning("The 'keywords' column was not found in your dataset. Keyword search will not be effective.")
        df['keywords_list'] = [[] for _ in range(len(df))]
    else:
        def parse_keywords(cell):
            if pd.isna(cell) or not str(cell).strip(): # Check for NaN or empty string after stripping
                return []
            try:
                # Try literal_eval for strings like "[{'id': 123, 'name': 'rescue'}, ...]"
                lst = ast.literal_eval(str(cell))
                if isinstance(lst, list) and all(isinstance(i, dict) and 'name' in i for i in lst):
                    return [str(item['name']).strip().lower() for item in lst if str(item['name']).strip()]
                elif isinstance(lst, list) and all(isinstance(i, str) for i in lst):
                    return [str(item).strip().lower() for item in lst if str(item).strip()]
            except (ValueError, SyntaxError, TypeError):
                pass # Not a literal list, proceed to comma split or single string handling
            
            # Fallback to comma-separated string (e.g., "rescue, mission") or single string
            if isinstance(cell, str):
                return [k.strip().lower() for k in cell.split(',') if k.strip()]
            
            return [str(cell).strip().lower()] if str(cell).strip() else []

        df['keywords_list'] = df['keywords'].apply(parse_keywords)

    if 'genres' in df.columns:
        # Create an inverted map for name -> id lookup
        name_to_id_map = {name: id for id, name in genre_map.items()}

        def parse_genres(cell):
            if pd.isna(cell) or not isinstance(cell, str):
                return []
            
            # First, try to parse as a literal (for robust handling of different dataset formats)
            try:
                lst = ast.literal_eval(cell)
                if isinstance(lst, list) and all(isinstance(i, dict) and 'id' in i for i in lst):
                    return [i['id'] for i in lst]
                elif isinstance(lst, list) and all(isinstance(i, (int, float)) for i in lst):
                    return [int(i) for i in lst]
            except (ValueError, SyntaxError):
                # If ast.literal_eval fails, it's likely a comma-separated string of names
                pass # Continue to the next parsing method
            
            # If literal_eval failed, try parsing as comma-separated names
            parsed_ids = []
            for name in [n.strip() for n in cell.split(',')]:
                if name: # Ensure name is not empty after stripping
                    genre_id = name_to_id_map.get(name)
                    if genre_id is not None:
                        parsed_ids.append(genre_id)
            return parsed_ids

        df['genre_ids'] = df['genres'].apply(parse_genres)
    else:
        st.warning("'genres' column NOT FOUND in DataFrame. Genre filters will not work.")
        df['genre_ids'] = [[] for _ in range(len(df))]
    return df

def get_genre_names(genre_ids, genre_map):
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

@st.cache_data # Cache the image data
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
    st.markdown(f"Rating: {movie['vote_average']:.1f}") # Format rating to 1 decimal place
    
    genres = get_genre_names(movie['genre_ids'], genre_map)
    st.markdown(f"Genres: {genres}")
    lang = lang_code_map.get(movie['original_language'], movie['original_language'])
    st.markdown(f"Language: {lang}")
    if movie['overview']:
        st.markdown(f"Overview: {movie['overview']}")
    fav_text = "Remove from Favorites" if is_fav else "Add to Favorites"
    return st.button(fav_text, key=f"fav_{movie['id']}")

# ======= MAIN APP =======

def main():
    # Initialize session state variables
    if 'user_age' not in st.session_state:
        st.session_state.user_age = None

    # Show age verification "pop-up" if age is not set
    if st.session_state.user_age is None:
        st.markdown("<h1 style='text-align: center; color: red;'>⚠️ Age Verification ⚠️</h1>", unsafe_allow_html=True)
        st.markdown("<h3 style='text-align: center;'>Please enter your age to proceed.</h3>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center;'>Content will be filtered based on your age.</p>", unsafe_allow_html=True)
        
        st.markdown("---")
        
        col1, col2, col3 = st.columns([1,3,1])
        with col2:
            current_age_input = st.number_input(
                "Your Age:",
                min_value=1,
                max_value=120,
                value=18, # Default to 18
                step=1,
                key="age_input"
            )
            
            st.markdown("---")
            
            if st.button("Continue", key="confirm_age_button", use_container_width=True):
                st.session_state.user_age = current_age_input
                st.session_state.page = 0 # Reset page on first entry
                safe_rerun()
            
            st.info("By clicking 'Continue', you acknowledge that content may be displayed according to your entered age.")
            
        return # Stop execution of main app logic until age is set

    # --- Main Application Logic (only runs if user_age is set) ---
    st.title("Today in Movie History - Movie Explorer")

    genre_map = get_genre_map(API_KEY)
    if not genre_map:
        st.error("Cannot load genre data from TMDb. Please try again later. (Genre Map Empty)")
        return
    else:
        st.sidebar.info(f"Successfully loaded {len(genre_map)} genres from TMDb.") 

    # Pass genre_map to load_data
    df = load_data(genre_map) 

    st.sidebar.header("Content Settings")
    
    # Get user's age from session state
    user_age = st.session_state.user_age
    st.sidebar.write(f"Your Age: **{user_age}**")
    
    allow_adult_content = False # Default to False
    checkbox_disabled = False
    
    if user_age < 18:
        allow_adult_content = False # Force to False for underage users
        checkbox_disabled = True
        st.sidebar.info("Adult content is automatically filtered for your age.")
    else: # user_age >= 18
        allow_adult_content = st.sidebar.checkbox(
            "Show Adult Content (requires 18+ consent)",
            value=st.session_state.get('allow_adult_content', False),
            key="allow_adult_content_checkbox",
            on_change=reset_page,
            disabled=False # Enable checkbox for adult users
        )
        st.session_state.allow_adult_content = allow_adult_content # Store the state for persistence
        
        if not allow_adult_content:
            st.sidebar.info("Adult content is currently filtered out. Check 'Show Adult Content' to view it.")

    # Apply adult content filter based on user_age and checkbox state
    if not allow_adult_content: # This will be true if user_age < 18 or if >=18 and checkbox is unchecked
        df = df[df['adult'] == False]

    # --- Global Search Bar ---
    st.subheader("Global Search")
    search_query = st.text_input("Search movie titles or keywords:", key="global_search_input", on_change=reset_page)
    
    # --- Release Year Filter ---
    # Get all available release years from the dataset, filter out future years
    current_year = datetime.now().year
    all_release_years = sorted(df['release_date'].dt.year.dropna().unique().astype(int).tolist(), reverse=True)
    # Filter years to be less than or equal to the current year
    all_release_years = [year for year in all_release_years if year <= current_year]

    selected_year = st.selectbox(
        "Filter by Release Year:",
        options=[''] + all_release_years, # Add an empty option for no year filter
        key="release_year_filter",
        on_change=reset_page
    )

    movies_to_show_initial = df.copy() # Start with the age-filtered DataFrame

    # Apply global search filter
    if search_query:
        search_query_lower = search_query.lower()
        # Filter by title, original_title, or keywords (case-insensitive)
        movies_to_show_initial = movies_to_show_initial[
            movies_to_show_initial['title'].fillna('').str.lower().str.contains(search_query_lower) |
            movies_to_show_initial['original_title'].fillna('').str.lower().str.contains(search_query_lower) |
            movies_to_show_initial['keywords_list'].apply(lambda x: any(search_query_lower in k for k in x) if isinstance(x, list) else False)
        ]
        if movies_to_show_initial.empty:
            st.warning(f"No movies found matching '{search_query}'.")
            return 
    
    # Apply release year filter
    if selected_year:
        movies_to_show_initial = movies_to_show_initial[movies_to_show_initial['release_date'].dt.year == selected_year]
        if movies_to_show_initial.empty:
            st.warning(f"No movies found for year {selected_year} after applying other filters.")
            return

    # If no search query and no year selected, revert to today's date or fallback logic
    # This block is only entered if NO global search or year filter was applied
    if not search_query and not selected_year:
        st.subheader("Movies Released Today in History")
        filtered_by_date, used_month, used_day = find_movies_with_fallback(movies_to_show_initial)
        if filtered_by_date.empty:
            st.info("No movies found released recently on this date. Displaying all available movies (after age/genre/language/rating filters).")
            movies_to_show_initial = movies_to_show_initial.copy() # Use the currently filtered df
        else:
            if used_month is not None and used_day is not None:
                st.write(f"Showing movies released on {datetime(1, used_month, used_day).strftime('%B %d')}")
            movies_to_show_initial = filtered_by_date.copy()


    st.sidebar.header("Filters")

    genres_list = sorted(set(genre_map.values()))
    selected_genre = st.sidebar.selectbox(
        "Genre",
        options=[""] + genres_list,
        key="genre_filter",
        on_change=reset_page
    )

    langs_codes = sorted(df['original_language'].dropna().unique())
    langs_display_list = sorted({lang_code_map.get(code, code) for code in langs_codes})
    selected_language = st.sidebar.selectbox(
        "Language",
        options=[""] + langs_display_list,
        key="language_filter",
        on_change=reset_page
    )

    min_rating = st.sidebar.slider(
        "Minimum Rating",
        0.0, 10.0, 0.0, step=0.1,
        key="rating_filter",
        on_change=reset_page
    )

    movies_to_show = movies_to_show_initial.copy()

    # Apply genre filter
    if selected_genre:
        selected_genre_id = None
        for g_id, g_name in genre_map.items():
            if g_name == selected_genre:
                selected_genre_id = g_id
                break
        if selected_genre_id is not None:
            movies_to_show = movies_to_show[movies_to_show['genre_ids'].apply(lambda ids: selected_genre_id in ids)]

    # Apply language filter
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

    # --- Final Sorting (Always by Release Date, Newest First) ---
    # The 'sort by popularity' toggle has been removed as per your request.
    movies_to_show = movies_to_show.sort_values(by='release_date', ascending=False).reset_index(drop=True)


    if 'page' not in st.session_state:
        st.session_state.page = 0
    if 'favorites' not in st.session_state:
        st.session_state.favorites = set()

    total_movies = len(movies_to_show)
    page_size = 10
    max_page = max(0, (total_movies - 1) // page_size)

    # Adjust page if current page exceeds new max_page due to filtering
    if st.session_state.page > max_page:
        st.session_state.page = max_page
        # No need to rerun here, as the main script execution will continue with the updated page

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
        if st.button("Previous", disabled=(st.session_state.page == 0)):
            st.session_state.page -= 1
            safe_rerun()
    with col3:
        if st.button("Next", disabled=(st.session_state.page >= max_page)):
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

    st.sidebar.markdown("---")
    # Add a button to reset age if user wants to change it
    if st.sidebar.button("Reset Age / Change User"):
        st.session_state.user_age = None
        st.session_state.page = 0
        st.session_state.favorites = set() # Clear favorites on age reset
        if 'allow_adult_content' in st.session_state:
            del st.session_state.allow_adult_content # Clear stored adult content preference
        safe_rerun()
        
    if st.sidebar.button("🔄 Clear Cache and Reload Data"):
        st.cache_data.clear()
        if os.path.exists(CACHED_PARQUET_PATH):
            os.remove(CACHED_PARQUET_PATH) # Also delete the local parquet file
        safe_rerun()

if __name__ == "__main__":
    main()
