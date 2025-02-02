import os
import plexapi.server
import yaml
from datetime import datetime
import sys
import requests
from typing import Dict, List, Set, Optional, Tuple
from collections import Counter, defaultdict
import time
import webbrowser
import random
import json
from urllib.parse import quote
import re
from datetime import datetime, timedelta

__version__ = "0.9"
REPO_URL = "https://github.com/netplexflix/TV-Show-Recommendations-for-Plex"
API_VERSION_URL = f"https://api.github.com/repos/netplexflix/TV-Show-Recommendations-for-Plex/releases/latest"

# ANSI Color Codes
RED = '\033[91m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
CYAN = '\033[96m'
RESET = '\033[0m'

def get_full_language_name(lang_code: str) -> str:
    LANGUAGE_CODES = {
        'en': 'English',
        'es': 'Spanish',
        'fr': 'French',
        'de': 'German',
        'it': 'Italian',
        'zh': 'Chinese',
        'ja': 'Japanese',
        'ko': 'Korean',
        'pt': 'Portuguese',
        'ru': 'Russian',
        'ar': 'Arabic',
        'hi': 'Hindi',
        'bn': 'Bengali',
        'pa': 'Punjabi',
        'jv': 'Javanese',
        'vi': 'Vietnamese',
        'tr': 'Turkish',
        'nl': 'Dutch',
        'da': 'Danish',
        'sv': 'Swedish',
        'no': 'Norwegian',
        'fi': 'Finnish',
        'pl': 'Polish',
        'cs': 'Czech',
        'hu': 'Hungarian',
        'el': 'Greek',
        'he': 'Hebrew',
        'id': 'Indonesian',
        'ms': 'Malay',
        'th': 'Thai',
        'tl': 'Tagalog',
        # Add more as needed
    }
    return LANGUAGE_CODES.get(lang_code.lower(), lang_code.capitalize())

def check_version():
    try:
        response = requests.get(API_VERSION_URL)
        if response.status_code == 200:
            latest_release = response.json()
            latest_version = latest_release['tag_name'].lstrip('v')
            if latest_version > __version__:
                print(f"{YELLOW}A new version is available: v{latest_version}")
                print(f"You are currently running: v{__version__}")
                print(f"Please visit {REPO_URL}/releases to download the latest version.{RESET}")
            else:
                print(f"{GREEN}You are running the latest version (v{__version__}){RESET}")
        else:
            print(f"{YELLOW}Unable to check for updates. Status code: {response.status_code}{RESET}")
    except Exception as e:
        print(f"{YELLOW}Unable to check for updates: {str(e)}{RESET}")

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class PlexTVRecommender:
    def __init__(self, config_path: str):
        # Initialize default attributes
        self.cached_watched_count = 0
        self.cached_unwatched_count = 0
        self.cached_library_show_count = 0
        self.watched_data_counters = {}
        self.cached_unwatched_shows = []
        self.plex_tmdb_cache = {}
        self.tmdb_keywords_cache = {}
        self.synced_trakt_history = {}
        self.synced_show_ids = set()

        self.use_tmdb_keywords = False
        self.tmdb_api_key = None

    
        print("Initializing recommendation system...")
        self.config = self._load_config(config_path)
        
        # Load TMDB config early
        tmdb_config = self.config.get('TMDB', {})
        self.use_tmdb_keywords = tmdb_config.get('use_TMDB_keywords', False)
        self.tmdb_api_key = tmdb_config.get('api_key', None)
    
        print("Connecting to Plex server...")
        self.plex = self._init_plex()
        print(f"Connected to Plex successfully!\n")
        print(f"{YELLOW}Checking Cache...{RESET}")
    
        # Load general config
        general_config = self.config.get('general', {})
        self.confirm_operations = general_config.get('confirm_operations', False)
        self.limit_plex_results = general_config.get('limit_plex_results', 10)
        self.limit_trakt_results = general_config.get('limit_trakt_results', 10)
        self.show_summary = general_config.get('show_summary', False)
        self.plex_only = general_config.get('plex_only', False)
        self.show_cast = general_config.get('show_cast', False)
        self.show_studio = general_config.get('show_studio', False)
        self.show_language = general_config.get('show_language', False)
        self.show_imdb_link = general_config.get('show_imdb_link', False)
        self.show_imdb_rating = general_config.get('show_imdb_rating', False)
        
        # Set up excluded genres
        exclude_genre_str = general_config.get('exclude_genre', '')
        self.exclude_genres = [g.strip().lower() for g in exclude_genre_str.split(',') if g.strip()] if exclude_genre_str else []
    
        # Load weights
        weights_config = self.config.get('weights', {})
        self.weights = {
            'genre_weight': float(weights_config.get('genre_weight', 0.25)),
            'studio_weight': float(weights_config.get('studio_weight', 0.20)),
            'actor_weight': float(weights_config.get('actor_weight', 0.20)),
            'language_weight': float(weights_config.get('language_weight', 0.10)),
            'keyword_weight': float(weights_config.get('keyword_weight', 0.25))
        }
    
        # Validate weights
        total_weight = sum(self.weights.values())
        if not abs(total_weight - 1.0) < 1e-6:
            print(f"{YELLOW}Warning: Weights sum to {total_weight}, expected 1.0.{RESET}")
    
        # Set up cache paths
        self.cache_dir = os.path.join(os.path.dirname(__file__), "cache")
        os.makedirs(self.cache_dir, exist_ok=True)
        
        self.watched_cache_path = os.path.join(self.cache_dir, "watched_data_cache.json")
        self.unwatched_cache_path = os.path.join(self.cache_dir, "unwatched_data_cache.json")
        self.trakt_sync_cache_path = os.path.join(self.cache_dir, "trakt_sync_cache.json")
    
        # Get current counts from Plex
        self.library_title = self.config['plex'].get('TV_library_title', 'TV Shows')
        self.library_shows = self._get_library_shows_set()
        self.current_watched_count = self._get_watched_count()
        self.current_unwatched_count = len(self.library_shows) - self.current_watched_count
    
        # Load and validate watched shows cache
        cache_valid = False
        if os.path.exists(self.watched_cache_path):
            try:
                with open(self.watched_cache_path, 'r', encoding='utf-8') as f:
                    watched_cache = json.load(f)
                    cached_count = watched_cache.get('watched_count', 0)
                    if cached_count == self.current_watched_count:
                        self.cached_watched_count = cached_count
                        self.watched_data_counters = watched_cache.get('watched_data_counters', {})
                        self.plex_tmdb_cache = watched_cache.get('plex_tmdb_cache', {})
                        self.tmdb_keywords_cache = watched_cache.get('tmdb_keywords_cache', {})
                        if self.watched_data_counters:
                            cache_valid = True
                            
            except Exception as e:
                print(f"{YELLOW}Error loading watched cache: {e}{RESET}")

        # Load and validate unwatched shows cache
        if os.path.exists(self.unwatched_cache_path):
            try:
                with open(self.unwatched_cache_path, 'r', encoding='utf-8') as f:
                    unwatched_cache = json.load(f)
                    self.cached_unwatched_count = unwatched_cache.get('unwatched_count', 0)
                    self.cached_library_show_count = unwatched_cache.get('library_show_count', 0)
                    self.cached_unwatched_shows = unwatched_cache.get('unwatched_show_details', [])
            except Exception as e:
                print(f"{YELLOW}Error loading unwatched cache: {e}{RESET}")
				
        # Use cache or rebuild watched data
        if cache_valid:
            print("Watched shows count unchanged. Using cached data.")
            self.watched_data = self.watched_data_counters
        else:
            print("Watched shows count changed or cache invalid. Rebuilding profile.")
            self.watched_data = self._get_watched_shows_data()
            self.watched_data_counters = self.watched_data
            self.cached_watched_count = self.current_watched_count
            self._save_watched_cache()

        # Load Trakt sync cache
        self.synced_show_ids = self._load_trakt_sync_cache()
		
        # Initialize Trakt
        trakt_config = self.config.get('trakt', {})
        self.sync_watch_history = trakt_config.get('sync_watch_history', False)
        self.trakt_headers = {
            'Content-Type': 'application/json',
            'trakt-api-version': '2',
            'trakt-api-key': trakt_config['client_id']
        }
        if 'access_token' in trakt_config:
            self.trakt_headers['Authorization'] = f"Bearer {trakt_config['access_token']}"
        else:
            self._authenticate_trakt()
    
        # Initialize Sonarr
        self.sonarr_config = self.config.get('sonarr', {})
 
    # ------------------------------------------------------------------------
    # CONFIG / PLEX SETUP
    # ------------------------------------------------------------------------
    def _load_config(self, config_path: str) -> Dict:
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                print(f"Successfully loaded configuration from {config_path}")
                return config
        except Exception as e:
            print(f"{RED}Error loading config from {config_path}: {e}{RESET}")
            raise

    def _init_plex(self) -> plexapi.server.PlexServer:
        try:
            return plexapi.server.PlexServer(
                self.config['plex']['url'],
                self.config['plex']['token']
            )
        except Exception as e:
            print(f"{RED}Error connecting to Plex server: {e}{RESET}")
            raise

    def _authenticate_trakt(self):
        try:
            response = requests.post(
                'https://api.trakt.tv/oauth/device/code',
                headers={'Content-Type': 'application/json'},
                json={'client_id': self.config['trakt']['client_id']}
            )
            
            if response.status_code == 200:
                data = response.json()
                device_code = data['device_code']
                user_code = data['user_code']
                verification_url = data['verification_url']
                
                print(f"\n{GREEN}Please visit {verification_url} and enter code: {CYAN}{user_code}{RESET}")
                print("Waiting for authentication...")
                webbrowser.open(verification_url)
                
                poll_interval = data['interval']
                expires_in = data['expires_in']
                start_time = time.time()
                
                while time.time() - start_time < expires_in:
                    time.sleep(poll_interval)
                    token_response = requests.post(
                        'https://api.trakt.tv/oauth/device/token',
                        headers={'Content-Type': 'application/json'},
                        json={
                            'code': device_code,
                            'client_id': self.config['trakt']['client_id'],
                            'client_secret': self.config['trakt']['client_secret']
                        }
                    )
                    
                    if token_response.status_code == 200:
                        token_data = token_response.json()
                        self.config['trakt']['access_token'] = token_data['access_token']
                        self.trakt_headers['Authorization'] = f"Bearer {token_data['access_token']}"
                        
                        with open(os.path.join(os.path.dirname(__file__), 'config.yml'), 'w') as f:
                            yaml.dump(self.config, f)
                            
                        print(f"{GREEN}Successfully authenticated with Trakt!{RESET}")
                        return
                    elif token_response.status_code != 400:
                        print(f"{RED}Error getting token: {token_response.status_code}{RESET}")
                        return
                print(f"{RED}Authentication timed out{RESET}")
            else:
                print(f"{RED}Error getting device code: {response.status_code}{RESET}")
        except Exception as e:
            print(f"{RED}Error during Trakt authentication: {e}{RESET}")

    # ------------------------------------------------------------------------
    # CACHING LOGIC
    # ------------------------------------------------------------------------
    def _load_trakt_sync_cache(self) -> Set[int]:
        """Load previously synced show IDs"""
        if not os.path.exists(self.trakt_sync_cache_path):
            return set()
        try:
            with open(self.trakt_sync_cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                synced_ids = set(data.get('synced_show_ids', []))
                return synced_ids
        except Exception as e:
            print(f"{YELLOW}Error loading Trakt sync cache: {e}{RESET}")
            return set()
			
    def _save_watched_cache(self):
        data = {
            'watched_count': self.cached_watched_count,  # Now stores show count
            'watched_show_ids': list(self._get_watched_show_ids()),  # Store show IDs
            'watched_data_counters': self.watched_data_counters,
            'plex_tmdb_cache': self.plex_tmdb_cache,
            'tmdb_keywords_cache': self.tmdb_keywords_cache
        }
        try:
            with open(self.watched_cache_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            print(f"{YELLOW}Error saving watched cache: {e}{RESET}")
    
    def _save_unwatched_cache(self):
        data = {
            'library_show_count': self.cached_library_show_count,
            'unwatched_count': self.cached_unwatched_count,
            'unwatched_show_details': self.cached_unwatched_shows
        }
        try:
            with open(self.unwatched_cache_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, cls=DateTimeEncoder)
        except Exception as e:
            print(f"{YELLOW}Error saving unwatched cache: {e}{RESET}")

    def _save_trakt_sync_cache(self):
        """Save synced show IDs to cache using class attribute"""
        try:
            with open(self.trakt_sync_cache_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'synced_show_ids': list(self.synced_show_ids),
                    'last_sync': datetime.now().isoformat()
                }, f, indent=4)
        except Exception as e:
            print(f"{YELLOW}Error saving Trakt sync cache: {e}{RESET}")
			
    def _save_cache(self):
        self._save_watched_cache()
        self._save_unwatched_cache()
        self._save_trakt_sync_cache()

    def _get_watched_count(self) -> int:
        return len(self._get_watched_show_ids())

    # ------------------------------------------------------------------------
    # PATH HANDLING
    # ------------------------------------------------------------------------
    def _map_path(self, path: str) -> str:
        try:
            if not self.config.get('paths'):
                return path
                
            mappings = self.config['paths'].get('path_mappings')
            if not mappings:
                return path
                
            platform = self.config['paths'].get('platform', '').lower()
            if platform == 'windows':
                path = path.replace('/', '\\')
            else:
                path = path.replace('\\', '/')
                
            for local_path, remote_path in mappings.items():
                if path.startswith(local_path):
                    mapped_path = path.replace(local_path, remote_path, 1)
                    print(f"{YELLOW}Mapped path: {path} -> {mapped_path}{RESET}")
                    return mapped_path
            return path
            
        except Exception as e:
            print(f"{YELLOW}Warning: Path mapping failed: {e}. Using original path.{RESET}")
            return path

    # ------------------------------------------------------------------------
    # LIBRARY UTILITIES
    # ------------------------------------------------------------------------
    def _get_library_shows_set(self) -> Set[int]:
        try:
            shows = self.plex.library.section(self.library_title)
            return {show.ratingKey for show in shows.all()}
        except Exception as e:
            print(f"{RED}Error getting library shows: {e}{RESET}")
            return set()

    def _get_watched_show_ids(self) -> Set[int]:
        try:
            shows_section = self.plex.library.section(self.library_title)
            watched_episodes = shows_section.searchEpisodes(unwatched=False)
            # Extract unique show IDs from watched episodes
            return {ep.grandparentRatingKey for ep in watched_episodes}
        except Exception as e:
            print(f"{RED}Error getting watched episodes: {e}{RESET}")
            return set()

    def _is_show_in_library(self, title: str, year: Optional[int]) -> bool:
        return (title.lower(), year) in self.library_shows

    # ------------------------------------------------------------------------
    # TMDB HELPER METHODS
    # ------------------------------------------------------------------------
    def _get_tmdb_id_via_imdb(self, plex_show) -> Optional[int]:
        imdb_id = self._get_plex_show_imdb_id(plex_show)
        if not imdb_id or not self.tmdb_api_key:
            return None
    
        try:
            url = f"https://api.themoviedb.org/3/find/{imdb_id}"
            params = {'api_key': self.tmdb_api_key, 'external_source': 'imdb_id'}
            resp = requests.get(url, params=params)
            resp.raise_for_status()
            return resp.json().get('tv_results', [{}])[0].get('id')
        except Exception as e:
            print(f"{YELLOW}IMDb fallback failed: {e}{RESET}")
            return Non

    def _get_plex_show_tmdb_id(self, plex_show) -> Optional[int]:
        # Recursion guard and cache check
        if hasattr(plex_show, '_tmdb_fallback_attempted'):
            return self.plex_tmdb_cache.get(plex_show.ratingKey)
        
        if plex_show.ratingKey in self.plex_tmdb_cache:
            return self.plex_tmdb_cache[plex_show.ratingKey]
    
        tmdb_id = None
        show_title = plex_show.title
        show_year = getattr(plex_show, 'year', None)
    
        # Method 1: Check Plex GUIDs
        if hasattr(plex_show, 'guids'):
            for guid in plex_show.guids:
                if 'themoviedb' in guid.id:
                    try:
                        tmdb_id = int(guid.id.split('themoviedb://')[1].split('?')[0])
                        break
                    except (ValueError, IndexError) as e:
                        continue
    
        # Method 2: TMDB API Search
        if not tmdb_id and self.tmdb_api_key:
            try:
                params = {
                    'api_key': self.tmdb_api_key,
                    'query': show_title,
                    'include_adult': False
                }
                if show_year:
                    params['first_air_date_year'] = show_year
    
                resp = requests.get(
                    "https://api.themoviedb.org/3/search/tv",
                    params=params,
                    timeout=10
                )
                resp.raise_for_status()
                
                results = resp.json().get('results', [])
                if results:
                    # Find exact match considering year
                    exact_match = next(
                        (r for r in results 
                         if r.get('name', '').lower() == show_title.lower()
                         and str(r.get('first_air_date', '')[:4]) == str(show_year)),
                        None
                    )
                    
                    tmdb_id = exact_match['id'] if exact_match else results[0]['id']
    
            except Exception as e:
                print(f"{YELLOW}TMDB search failed for {show_title}: {e}{RESET}")
    
        # Method 3: Single Fallback Attempt via IMDb
        if not tmdb_id and not hasattr(plex_show, '_tmdb_fallback_attempted'):
            plex_show._tmdb_fallback_attempted = True  # Prevent recursion
            tmdb_id = self._get_tmdb_id_via_imdb(plex_show)
    
        # Update cache even if None to prevent repeat lookups
        self.plex_tmdb_cache[plex_show.ratingKey] = tmdb_id
        return tmdb_id

    def _get_plex_show_imdb_id(self, plex_show) -> Optional[str]:
        if not plex_show.guid:
            return None
        guid = plex_show.guid
        if guid.startswith('imdb://'):
            return guid.split('imdb://')[1]
        
        tmdb_id = self._get_plex_show_tmdb_id(plex_show)
        if not tmdb_id:
            return None
        try:
            url = f"https://api.themoviedb.org/3/tv/{tmdb_id}"
            params = {'api_key': self.tmdb_api_key}
            resp = requests.get(url, params=params)
            if resp.status_code == 200:
                data = resp.json()
                return data.get('external_ids', {}).get('imdb_id')
            else:
                print(f"{YELLOW}Failed to fetch IMDb ID from TMDB for show '{plex_show.title}'. Status Code: {resp.status_code}{RESET}")
        except Exception as e:
            print(f"{YELLOW}Error fetching IMDb ID for TMDB ID {tmdb_id}: {e}{RESET}")
        return None

    def _get_tmdb_keywords_for_id(self, tmdb_id: int) -> Set[str]:
        if not tmdb_id or not self.use_tmdb_keywords or not self.tmdb_api_key:
            return set()

        if tmdb_id in self.tmdb_keywords_cache:
            return set(self.tmdb_keywords_cache[tmdb_id])

        kw_set = set()
        try:
            url = f"https://api.themoviedb.org/3/tv/{tmdb_id}/keywords"
            params = {'api_key': self.tmdb_api_key}
            resp = requests.get(url, params=params)
            if resp.status_code == 200:
                data = resp.json()
                keywords = data.get('results', [])
                kw_set = {k['name'].lower() for k in keywords}
        except Exception as e:
            print(f"{YELLOW}Error fetching TMDB keywords for ID {tmdb_id}: {e}{RESET}")

        self.tmdb_keywords_cache[tmdb_id] = list(kw_set)
        return kw_set

    def _get_show_language(self, show) -> str:
        """Get show's primary audio language from first episode"""
        try:
            episodes = show.episodes()
            if not episodes:
                print(f"DEBUG: No episodes found")
                return "N/A"
    
            # Get and reload first episode
            episode = episodes[0]
            episode.reload()
            
            # Check media
            if not episode.media:
                return "N/A"
                
            for media in episode.media:
                for part in media.parts:
                    # Use audioStreams() method
                    audio_streams = part.audioStreams()
                    
                    if audio_streams:
                        # Get primary audio stream
                        audio = audio_streams[0]                     
                        # Try different language attributes
                        lang_code = (
                            getattr(audio, 'languageTag', None) or
                            getattr(audio, 'language', None)
                        )
                        if lang_code:
                            return get_full_language_name(lang_code)
                    else:
                        print(f"DEBUG: No audio streams found in part")
                        
        except Exception as e:
            print(f"DEBUG: Language detection failed for {show.title}: {str(e)}")
        return "N/A"

    def _extract_genres(self, show) -> List[str]:
        """Safely extract genres from show"""
        genres = []
        try:
            if not hasattr(show, 'genres') or not show.genres:
                return genres
                
            for genre in show.genres:
                if isinstance(genre, plexapi.media.Genre):
                    if hasattr(genre, 'tag'):
                        genres.append(genre.tag.lower())
                elif isinstance(genre, str):
                    genres.append(genre.lower())
                else:
                    print(f"DEBUG: Unknown genre type for {show.title}: {type(genre)}")
                    
        except Exception as e:
            print(f"DEBUG: Error extracting genres for {show.title}: {str(e)}")
        return genres

    def _get_watched_shows_data(self) -> Dict:
        print(f"\n{YELLOW}Fetching watched shows from Plex library...{RESET}")
        genre_counter = Counter()
        studio_counter = Counter()
        actor_counter = Counter()
        tmdb_keyword_counter = Counter()
        language_counter = Counter()

        try:
            shows_section = self.plex.library.section(self.library_title)
            watched_shows = shows_section.search(unwatched=False)
            total_watched = len(watched_shows)

            print(f"Found {total_watched} watched shows. Building frequency data...")
            for i, show in enumerate(watched_shows, start=1):
                self._show_progress("Analyzing watched shows", i, total_watched)

                user_rating = getattr(show, 'userRating', None)
                if user_rating is not None:
                    rating_weight = float(user_rating) / 10.0
                    rating_weight = min(max(rating_weight, 0.1), 1.0)
                else:
                    rating_weight = 0.5

                if hasattr(show, 'genres') and show.genres:
                    for g in show.genres:
                        if isinstance(g, plexapi.media.Genre) and hasattr(g, 'tag'):
                            genre_counter[g.tag.lower()] += rating_weight
                        elif isinstance(g, str):
                            genre_counter[g.lower()] += rating_weight

                if hasattr(show, 'studio') and show.studio:
                    studio_counter[show.studio.lower()] += rating_weight

                if hasattr(show, 'roles') and show.roles:
                    for a in show.roles:
                        actor_counter[a.tag] += rating_weight

                if self.show_language:
                    try:
                        language = self._get_show_language(show)
                        language_counter[language.lower()] += rating_weight
                    except Exception as e:
                        print(f"{YELLOW}Error getting language for '{show.title}': {e}{RESET}")

                if self.use_tmdb_keywords and self.tmdb_api_key:
                    tmdb_id = self._get_plex_show_tmdb_id(show)
                    if tmdb_id:
                        keywords = self._get_tmdb_keywords_for_id(tmdb_id)
                        for kw in keywords:
                            tmdb_keyword_counter[kw] += rating_weight

            print()

        except plexapi.exceptions.BadRequest as e:
            print(f"{RED}Error gathering watched shows data: {e}{RESET}")

        return {
            'genres': dict(genre_counter),
            'studio': dict(studio_counter),
            'actors': dict(actor_counter),
            'tmdb_keywords': dict(tmdb_keyword_counter),
            'languages': dict(language_counter)
        }

    def _show_progress(self, prefix: str, current: int, total: int):
        pct = int((current / total) * 100)
        msg = f"\r{prefix}: {current}/{total} ({pct}%)"
        sys.stdout.write(msg)
        sys.stdout.flush()
        if current == total:
            sys.stdout.write("\n")

    # ------------------------------------------------------------------------
    # TRAKT SYNC: BATCHED
    # ------------------------------------------------------------------------
    def _sync_plex_watched_to_trakt(self):
        """Sync watched shows to Trakt in chunks"""
        if not self.sync_watch_history:
            return
    
        print(f"{YELLOW}Checking Trakt sync status...{RESET}")
        all_watched_show_ids = self._get_watched_show_ids()
        shows_to_sync = list(all_watched_show_ids - self.synced_show_ids)
        #print(f"DEBUG: Total watched shows: {len(all_watched_show_ids)}")
        #print(f"DEBUG: Already synced shows: {len(self.synced_show_ids)}")
        #print(f"DEBUG: Shows to sync: {len(shows_to_sync)}")
    
        if not shows_to_sync:
            print(f"All watched shows already synced to Trakt.")
            return
    
        chunk_size = 100
        shows_section = self.plex.library.section(self.library_title)
        total_shows = len(shows_to_sync)
        synced_in_session = 0
    
        for chunk_start in range(0, total_shows, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_shows)
            chunk = shows_to_sync[chunk_start:chunk_end]
            chunk_payload = {"episodes": []}  # Correct root level key
            chunk_successful_ids = set()
            
            print(f"\nProcessing chunk {chunk_start + 1} to {chunk_end} of {total_shows}")
            
            for i, show_id in enumerate(chunk, 1):
                try:
                    show = shows_section.fetchItem(show_id)
                    progress = (chunk_start + i) / total_shows * 100
                    print(f"\rProgress: {progress:.1f}%", end='', flush=True)
                    
                    all_episodes = show.episodes()
                    watched_episodes = [ep for ep in all_episodes if ep.isWatched]
                    
                    if not watched_episodes:
                        continue
                        
                    first_episode = watched_episodes[0]
                    last_watched = getattr(first_episode, 'lastViewedAt', None)
                    if not last_watched:
                        continue
    
                    # Get show identifiers
                    imdb_id = None
                    tvdb_id = None
                    if hasattr(show, 'guids'):
                        for guid in show.guids:
                            if 'imdb://' in guid.id:
                                imdb_id = guid.id.replace('imdb://', '')
                            elif 'tvdb://' in guid.id:
                                tvdb_id = guid.id.replace('tvdb://', '')
    
                    # Format episode data for Trakt
                    episode_data = {
                        "watched_at": last_watched.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "ids": {
                            "tvdb": tvdb_id,
                            "imdb": imdb_id
                        }
                    }
    
                    chunk_payload["episodes"].append(episode_data)
                    chunk_successful_ids.add(show_id)
                    
                except Exception as e:
                    print(f"\n{YELLOW}Error processing {show.title}: {e}{RESET}")
                    continue
    
            if chunk_payload["episodes"]:
                print(f"\nDEBUG: Sending chunk with {len(chunk_payload['episodes'])} episodes")
                try:
                    response = requests.post(
                        "https://api.trakt.tv/sync/history",
                        headers=self.trakt_headers,
                        json=chunk_payload
                    )
                    print(f"DEBUG: Trakt API Response: {response.status_code}")
                    print(f"DEBUG: Response content: {response.text[:200]}")
                    response.raise_for_status()
                    
                    self.synced_show_ids.update(chunk_successful_ids)
                    synced_in_session += len(chunk_successful_ids)
                    print(f"\n{GREEN}Successfully synced chunk to Trakt{RESET}")
                    self._save_trakt_sync_cache()
                    
                except Exception as e:
                    print(f"\n{RED}Error syncing chunk to Trakt: {e}{RESET}")
                    continue
    
        print(f"\n{GREEN}Sync complete. {synced_in_session} shows synced to Trakt.{RESET}")

    def calculate_show_score(self, show) -> float:
        """Calculate recommendation score based on watched history"""
        try:
            user_genres = Counter(self.watched_data.get('genres', {}))
            user_studio = Counter(self.watched_data.get('studio', {}))
            user_acts = Counter(self.watched_data.get('actors', {}))
            user_kws = Counter(self.watched_data.get('tmdb_keywords', {}))
            user_langs = Counter(self.watched_data.get('languages', {}))
    
            weights = self.weights
            score = 0.0
    
            # Calculate maximum counts for normalization
            max_genre_count = max(user_genres.values()) if user_genres else 1
            max_studio_count = max(user_studio.values()) if user_studio else 1
            max_actor_count = max(user_acts.values()) if user_acts else 1
            max_keyword_count = max(user_kws.values()) if user_kws else 1
            max_language_count = max(user_langs.values()) if user_langs else 1
    
            # Genre Score
            show_genres = set(self._extract_genres(show))
            if show_genres:
                genre_scores = []
                for genre in show_genres:
                    genre_count = user_genres.get(genre, 0)
                    if genre_count > 0:
                        genre_scores.append(genre_count / max_genre_count)
                if genre_scores:
                    score += (sum(genre_scores) / len(genre_scores)) * weights.get('genre_weight', 0.25)
    
            # studio Score
            if hasattr(show, 'studio') and show.studio:
                studio_scores = []
                for studio in show.studio:
                    if hasattr(studio, 'tag') and studio.tag in user_studio:
                        studio_scores.append(user_studio[studio.tag] / max_studio_count)
                if studio_scores:
                    score += (sum(studio_scores) / len(studio_scores)) * weights.get('studio_weight', 0.20)
    
            # Actor Score
            if hasattr(show, 'roles') and show.roles:
                actor_scores = []
                matched_actors = 0
                for actor in show.roles:
                    if hasattr(actor, 'tag') and actor.tag in user_acts:
                        matched_actors += 1
                        actor_scores.append(user_acts[actor.tag] / max_actor_count)
                if matched_actors > 0:
                    actor_score = sum(actor_scores) / matched_actors
                    if matched_actors > 3:
                        actor_score *= (3 / matched_actors)  # Normalize if many matches
                    score += actor_score * weights.get('actor_weight', 0.20)
    
            # Language Score
            if self.show_language:
                try:
                    language = self._get_show_language(show)
                    if language != "N/A":
                        lang_count = user_langs.get(language.lower(), 0)
                        if lang_count > 0:
                            score += (lang_count / max_language_count) * weights.get('language_weight', 0.10)
                except Exception as e:
                    print(f"{YELLOW}Error calculating language score: {e}{RESET}")
    
            # TMDB Keywords Score
            if self.use_tmdb_keywords and self.tmdb_api_key:
                tmdb_id = self._get_plex_show_tmdb_id(show)
                if tmdb_id:
                    keywords = self._get_tmdb_keywords_for_id(tmdb_id)
                    keyword_scores = []
                    for kw in keywords:
                        count = user_kws.get(kw, 0)
                        if count > 0:
                            keyword_scores.append(count / max_keyword_count)
                    if keyword_scores:
                        score += (sum(keyword_scores) / len(keyword_scores)) * weights.get('keyword_weight', 0.25)
    
            return score
    
        except Exception as e:
            print(f"{YELLOW}Error calculating score for {show.title}: {e}{RESET}")
            return 0.0

    def get_show_details(self, show) -> Dict:
        try:
            show.reload()
            
            # Get IMDb ID and rating
            imdb_id = None
            imdb_rating = 0
            tmdb_keywords = []
            
            # Get IMDb ID
            if hasattr(show, 'guids'):
                for guid in show.guids:
                    if 'imdb://' in guid.id:
                        imdb_id = guid.id.replace('imdb://', '')
                        break
            
            # Get IMDb rating
            if self.show_imdb_rating and hasattr(show, 'ratings'):
                for rating in show.ratings:
                    if (getattr(rating, 'image', '') == 'imdb://image.rating' and 
                        getattr(rating, 'type', '') == 'audience'):
                        try:
                            imdb_rating = float(rating.value)
                            break
                        except (ValueError, AttributeError):
                            pass
                            
            # Get TMDB keywords if enabled
            if self.use_tmdb_keywords and self.tmdb_api_key:
                tmdb_id = self._get_plex_show_tmdb_id(show)
                if tmdb_id:
                    tmdb_keywords = list(self._get_tmdb_keywords_for_id(tmdb_id))
            
            # Build show info dictionary
            show_info = {
                'title': show.title,
                'year': getattr(show, 'year', None),
                'genres': self._extract_genres(show),
                'summary': getattr(show, 'summary', ''),
                'studio': getattr(show, 'studio', 'N/A'),
                'language': self._get_show_language(show),
                'imdb_id': imdb_id,
                'ratings': {
                    'imdb_rating': imdb_rating
                } if imdb_rating > 0 else {},
                'cast': [],
                'tmdb_keywords': tmdb_keywords
            }
            
            # Add cast if enabled
            if self.show_cast and hasattr(show, 'roles'):
                show_info['cast'] = [r.tag for r in show.roles[:3]]
                
            return show_info
                
        except Exception as e:
            print(f"{YELLOW}Error getting show details for {show.title}: {e}{RESET}")
            return {}

    def get_unwatched_library_shows(self) -> List[Dict]:
        print(f"\n{YELLOW}Fetching unwatched shows from Plex library...{RESET}")
        
        # Get current counts
        all_show_ids = self._get_library_shows_set()
        watched_show_ids = self._get_watched_show_ids()
        current_unwatched_count = len(all_show_ids) - len(watched_show_ids)
        
        # Check if cache is valid
        if (os.path.exists(self.unwatched_cache_path) and 
            self.cached_unwatched_count == current_unwatched_count and 
            self.cached_unwatched_shows):
            print("Unwatched shows count unchanged. Using cached data.")
            return self.cached_unwatched_shows
            
        print(f"Found {current_unwatched_count} unwatched shows. Analyzing profiles...")
        
        # Calculate unwatched show IDs
        unwatched_show_ids = all_show_ids - watched_show_ids
        shows_section = self.plex.library.section(self.library_title)
        
        unwatched_details = []
        excluded_count = 0
        
        for i, show_id in enumerate(unwatched_show_ids, start=1):
            try:
                show = shows_section.fetchItem(show_id)
                self._show_progress("Scanning unwatched", i, current_unwatched_count)
                
                info = self.get_show_details(show)
                show_genres = info.get('genres', [])
                
                if show_genres and any(g in self.exclude_genres for g in show_genres):
                    excluded_count += 1
                    continue
                    
                unwatched_details.append(info)
                
            except Exception as e:
                print(f"\nDEBUG: Error processing show ID {show_id}:")
                import traceback
                traceback.print_exc()
                continue
    
        print(f"\nProcessing complete:")
        print(f"- Excluded shows due to genre: {excluded_count}")
    
        # Update cache
        self.cached_library_show_count = len(all_show_ids)
        self.cached_unwatched_count = current_unwatched_count
        self.cached_unwatched_shows = unwatched_details
        self._save_unwatched_cache()
        
        return unwatched_details

    def get_trakt_recommendations(self) -> List[Dict]:
        print(f"\n{YELLOW}Fetching recommendations from Trakt...{RESET}")
        try:
            url = "https://api.trakt.tv/recommendations/tv"
            collected_recs = []
            page = 1
            per_page = 100  # Trakt's maximum allowed per page
    
            while len(collected_recs) < self.limit_trakt_results:
                response = requests.get(
                    url,
                    headers=self.trakt_headers,
                    params={
                        'limit': per_page,
                        'page': page,
                        'extended': 'full'
                    }
                )
    
                if response.status_code == 200:
                    shows = response.json()
                    if not shows:
                        break
    
                    # Randomize ratings to introduce variety
                    for s in shows:
                        if len(collected_recs) >= self.limit_trakt_results:
                            break
    
                        # Access the nested 'show' object
                        show_data = s.get('show', {})
                        if not show_data:
                            continue
    
                        base_rating = float(show_data.get('rating', 0.0))
                        show_data['_randomized_rating'] = base_rating + random.uniform(0, 0.5)
    
                    # Sort shows based on the randomized rating
                    shows.sort(key=lambda x: x.get('show', {}).get('_randomized_rating', 0), reverse=True)
    
                    for s in shows:
                        if len(collected_recs) >= self.limit_trakt_results:
                            break
    
                        show = s.get('show', {})
                        if not show:
                            continue
    
                        title = show.get('title', '').strip()
                        year = show.get('year', None)
    
                        if not title:
                            continue
    
                        if self._is_show_in_library(title, year):
                            continue
    
                        ratings = {
                            'imdb_rating': round(float(show.get('rating', 0)), 1),
                            'votes': show.get('votes', 0)
                        }
                        sd = {
                            'title': title,
                            'year': year,
                            'ratings': ratings,
                            'summary': show.get('overview', ''),
                            'genres': [g.lower() for g in show.get('genres', [])],
                            'cast': [],
                            'studio': "N/A",
                            'language': "N/A",
                            'imdb_id': show.get('ids', {}).get('imdb')
                        }
    
                        if any(g in self.exclude_genres for g in sd['genres']):
                            continue
    
                        tmdb_id = show.get('ids', {}).get('tmdb')
    
                        if tmdb_id and self.tmdb_api_key:
                            if self.show_language:
                                try:
                                    resp_lang = requests.get(
                                        f"https://api.themoviedb.org/3/tv/{tmdb_id}",
                                        params={'api_key': self.tmdb_api_key}
                                    )
                                    resp_lang.raise_for_status()
                                    d = resp_lang.json()
                                    if 'original_language' in d:
                                        sd['language'] = get_full_language_name(d['original_language'])
                                except Exception as e:
                                    print(f"{YELLOW}Error fetching language for '{title}': {e}{RESET}")
    
                            if self.show_cast or self.show_studio:
                                try:
                                    resp_credits = requests.get(
                                        f"https://api.themoviedb.org/3/tv/{tmdb_id}/credits",
                                        params={'api_key': self.tmdb_api_key}
                                    )
                                    resp_credits.raise_for_status()
                                    c_data = resp_credits.json()
    
                                    if self.show_cast and 'cast' in c_data:
                                        c_sorted = c_data['cast'][:3]
                                        sd['cast'] = [c['name'] for c in c_sorted]
    
                                    if self.show_studio and 'crew' in c_data:
                                        studio = [p for p in c_data['crew'] if p.get('job') == 'studio']
                                        if studio:
                                            sd['studio'] = studio[0]['name']
                                except Exception as e:
                                    print(f"{YELLOW}Error fetching credits for '{title}': {e}{RESET}")
    
                        collected_recs.append(sd)
    
                    if len(shows) < per_page:
                        break
    
                    page += 1
                else:
                    print(f"{RED}Error getting Trakt recommendations: {response.status_code}{RESET}")
                    if response.status_code == 401:
                        print(f"{YELLOW}Try re-authenticating with Trakt{RESET}")
                        self._authenticate_trakt()
                    break
    
            # Sort and limit the recommendations
            collected_recs.sort(key=lambda x: x.get('ratings', {}).get('imdb_rating', 0), reverse=True)
            random.shuffle(collected_recs)
            final_recs = collected_recs[:self.limit_trakt_results]
            print(f"Collected {len(final_recs)} Trakt recommendations after exclusions.")
            return final_recs
    
        except Exception as e:
            print(f"{RED}Error getting Trakt recommendations: {e}{RESET}")
            return []

    def get_recommendations(self) -> Dict[str, List[Dict]]:
        if self.sync_watch_history:
            self._sync_plex_watched_to_trakt()
            self._save_cache()
    
        plex_recs = self.get_unwatched_library_shows()
        if plex_recs:
            excluded_recs = [s for s in plex_recs if any(g in self.exclude_genres for g in s['genres'])]
            included_recs = [s for s in plex_recs if not any(g in self.exclude_genres for g in s['genres'])]

            if not included_recs:
                print(f"{YELLOW}No unwatched shows left after applying genre exclusions.{RESET}")
                plex_recs = []
            else:
                plex_recs = included_recs
                plex_recs.sort(
                    key=lambda x: (
                        x.get('ratings', {}).get('imdb_rating', 0),
                        x.get('similarity_score', 0)
                    ),
                    reverse=True
                )
                top_count = max(int(len(plex_recs) * 0.5), self.limit_plex_results)
                top_by_rating = plex_recs[:top_count]

                top_by_rating.sort(key=lambda x: x.get('similarity_score', 0), reverse=True)
                final_count = max(int(len(top_by_rating) * 0.3), self.limit_plex_results)
                final_pool = top_by_rating[:final_count]

                if final_pool:
                    plex_recs = random.sample(final_pool, min(self.limit_plex_results, len(final_pool)))
                else:
                    plex_recs = []
        else:
            plex_recs = []

        trakt_recs = []
        if not self.plex_only:
            trakt_recs = self.get_trakt_recommendations()

        print(f"\nRecommendation process completed!")
        return {
            'plex_recommendations': plex_recs,
            'trakt_recommendations': trakt_recs
        }

    def _user_select_recommendations(self, recommended_shows: List[Dict], operation_label: str) -> List[Dict]:
        prompt = (
            f"\nWhich recommendations would you like to {operation_label}?\n"
            "Enter 'all' or 'y' to select ALL,\n"
            "Enter 'none' or 'n' to skip them,\n"
            "Or enter a comma-separated list of numbers (e.g. 1,3,5). "
            "\nYour choice: "
        )
        choice = input(prompt).strip().lower()

        if choice in ("n", "no", "none", ""):
            print(f"{YELLOW}Skipping {operation_label} as per user choice.{RESET}")
            return []
        if choice in ("y", "yes", "all"):
            return recommended_shows

        indices_str = re.split(r'[,\s]+', choice)
        chosen = []
        for idx_str in indices_str:
            idx_str = idx_str.strip()
            if not idx_str.isdigit():
                print(f"{YELLOW}Skipping invalid index: {idx_str}{RESET}")
                continue
            idx = int(idx_str)
            if 1 <= idx <= len(recommended_shows):
                chosen.append(idx)
            else:
                print(f"{YELLOW}Skipping out-of-range index: {idx}{RESET}")

        if not chosen:
            print(f"{YELLOW}No valid indices selected, skipping {operation_label}.{RESET}")
            return []

        subset = []
        for c in chosen:
            subset.append(recommended_shows[c - 1])
        return subset

    def manage_plex_labels(self, recommended_shows: List[Dict]) -> None:
        if not recommended_shows:
            print(f"{YELLOW}No shows to add labels to.{RESET}")
            return

        if not self.config['plex'].get('add_label'):
            return

        if self.confirm_operations:
            selected_shows = self._user_select_recommendations(recommended_shows, "label in Plex")
            if not selected_shows:
                return
        else:
            selected_shows = recommended_shows

        try:
            shows_section = self.plex.library.section(self.library_title)
            label_name = self.config['plex'].get('label_name', 'Recommended')

            shows_to_update = []
            for rec in selected_shows:
                plex_show = next(
                    (s for s in shows_section.search(title=rec['title'])
                     if s.year == rec.get('year')), 
                    None
                )
                if plex_show:
                    plex_show.reload()
                    shows_to_update.append(plex_show)

            if not shows_to_update:
                print(f"{YELLOW}No matching shows found in Plex to add labels to.{RESET}")
                return

            if self.config['plex'].get('remove_previous_recommendations', False):
                print(f"{YELLOW}Finding shows with existing label: {label_name}{RESET}")
                labeled_shows = set(shows_section.search(label=label_name))
                shows_to_unlabel = labeled_shows - set(shows_to_update)
                for show in shows_to_unlabel:
                    current_labels = [label.tag for label in show.labels]
                    if label_name in current_labels:
                        show.removeLabel(label_name)
                        print(f"{YELLOW}Removed label from: {show.title}{RESET}")

            print(f"{YELLOW}Adding label to recommended shows...{RESET}")
            for show in shows_to_update:
                current_labels = [label.tag for label in show.labels]
                if label_name not in current_labels:
                    show.addLabel(label_name)
                    print(f"{GREEN}Added label to: {show.title}{RESET}")
                else:
                    print(f"{YELLOW}Label already exists on: {show.title}{RESET}")

            print(f"{GREEN}Successfully updated labels for recommended shows{RESET}")

        except Exception as e:
            print(f"{RED}Error managing Plex labels: {e}{RESET}")
            import traceback
            print(traceback.format_exc())

    def add_to_sonarr(self, recommended_shows: List[Dict]) -> None:
        if not recommended_shows:
            print(f"{YELLOW}No shows to add to Sonarr.{RESET}")
            return
    
        if not self.sonarr_config.get('add_to_sonarr'):
            return
    
        valid_options = ['all', 'none', 'firstseason']
        monitor_option = self.sonarr_config.get('monitor_option', 'all')
        search_missing = self.sonarr_config.get('search_missing', False)
        
        if monitor_option not in valid_options:
            print(f"{RED}Invalid monitor_option '{monitor_option}'. Using 'all'{RESET}")
            monitor_option = 'all'
    
        if self.confirm_operations:
            selected_shows = self._user_select_recommendations(recommended_shows, "add to Sonarr")
            if not selected_shows:
                return
            
            print(f"\nMonitoring options: {', '.join(valid_options)}")
            while True:
                choice = input(f"Choose monitoring [default: {monitor_option}]: ").strip().lower()
                if not choice:
                    break
                if choice in valid_options:
                    monitor_option = choice
                    break
                print(f"{RED}Invalid option. Valid choices: {', '.join(valid_options)}{RESET}")
    
        try:
            if 'sonarr' not in self.config:
                raise ValueError("Sonarr configuration missing from config file")
    
            required_fields = ['url', 'api_key', 'root_folder', 'quality_profile']
            missing_fields = [f for f in required_fields if f not in self.sonarr_config]
            if missing_fields:
                raise ValueError(f"Missing required Sonarr config fields: {', '.join(missing_fields)}")
    
            sonarr_url = self.sonarr_config['url'].rstrip('/')
            if '/api/' not in sonarr_url:
                sonarr_url += '/api/v3'
            
            headers = {
                'X-Api-Key': self.sonarr_config['api_key'],
                'Content-Type': 'application/json'
            }
            trakt_headers = self.trakt_headers
    
            try:
                test_response = requests.get(f"{sonarr_url}/system/status", headers=headers)
                test_response.raise_for_status()
            except requests.exceptions.RequestException as e:
                raise ValueError(f"Failed to connect to Sonarr: {str(e)}")
    
            tag_id = None
            if self.sonarr_config.get('sonarr_tag'):
                tags_response = requests.get(f"{sonarr_url}/tag", headers=headers)
                tags_response.raise_for_status()
                tags = tags_response.json()
                tag = next((t for t in tags if t['label'].lower() == self.sonarr_config['sonarr_tag'].lower()), None)
                if tag:
                    tag_id = tag['id']
                else:
                    tag_response = requests.post(
                        f"{sonarr_url}/tag",
                        headers=headers,
                        json={'label': self.sonarr_config['sonarr_tag']}
                    )
                    tag_response.raise_for_status()
                    tag_id = tag_response.json()['id']
    
            profiles_response = requests.get(f"{sonarr_url}/qualityprofile", headers=headers)
            profiles_response.raise_for_status()
            quality_profiles = profiles_response.json()
            desired_profile = next(
                (p for p in quality_profiles
                 if p['name'].lower() == self.sonarr_config['quality_profile'].lower()),
                None
            )
            if not desired_profile:
                available = [p['name'] for p in quality_profiles]
                raise ValueError(
                    f"Quality profile '{self.sonarr_config['quality_profile']}' not found. "
                    f"Available: {', '.join(available)}"
                )
            quality_profile_id = desired_profile['id']
    
            existing_response = requests.get(f"{sonarr_url}/series", headers=headers)
            existing_response.raise_for_status()
            existing_shows = existing_response.json()
            existing_tvdb_ids = {s['tvdbId'] for s in existing_shows}
    
            for show in selected_shows:
                try:
                    trakt_search_url = f"https://api.trakt.tv/search/show?query={quote(show['title'])}"
                    if show.get('year'):
                        trakt_search_url += f"&year={show['year']}"
    
                    trakt_response = requests.get(trakt_search_url, headers=trakt_headers)
                    trakt_response.raise_for_status()
                    trakt_results = trakt_response.json()
    
                    if not trakt_results:
                        print(f"{YELLOW}Show not found on Trakt: {show['title']}{RESET}")
                        continue
    
                    trakt_show = next(
                        (r for r in trakt_results
                         if r['show']['title'].lower() == show['title'].lower()
                         and r['show'].get('year') == show.get('year')),
                        trakt_results[0]
                    )
    
                    tmdb_id = trakt_show['show']['ids'].get('tmdb')
                    if not tmdb_id:
                        print(f"{YELLOW}No TMDB ID found for {show['title']}{RESET}")
                        continue
    
                    try:
                        tmdb_external_ids_url = f"https://api.themoviedb.org/3/tv/{tmdb_id}/external_ids"
                        tmdb_params = {'api_key': self.tmdb_api_key}
                        tmdb_resp = requests.get(tmdb_external_ids_url, params=tmdb_params)
                        tmdb_resp.raise_for_status()
                        external_ids = tmdb_resp.json()
                        tvdb_id = external_ids.get('tvdb_id')
                        
                        if not tvdb_id or tvdb_id <= 0:
                            print(f"{YELLOW}Invalid TVDB ID for {show['title']}: {tvdb_id}{RESET}")
                            continue
                    except Exception as e:
                        print(f"{RED}Error fetching TVDB ID for {show['title']}: {e}{RESET}")
                        continue
    
                    if tvdb_id in existing_tvdb_ids:
                        print(f"{YELLOW}Already in Sonarr: {show['title']}{RESET}")
                        continue
    
                    seasons = []
                    if monitor_option == 'firstSeason':
                        try:
                            tmdb_seasons_url = f"https://api.themoviedb.org/3/tv/{tmdb_id}"
                            tmdb_params = {'api_key': self.tmdb_api_key}
                            resp = requests.get(tmdb_seasons_url, params=tmdb_params)
                            if resp.status_code == 200:
                                show_data = resp.json()
                                seasons = [
                                    {
                                        'seasonNumber': s['season_number'],
                                        'monitored': s['season_number'] == 1
                                    } 
                                    for s in show_data.get('seasons', [])
                                    if s.get('season_number', -1) >= 0  # Exclude specials
                                ]
                        except Exception as e:
                            print(f"{YELLOW}Failed to get season data: {e}. Monitoring all.{RESET}")
                            monitor_option = 'all'
    
                    root_folder = self._map_path(self.sonarr_config['root_folder'].rstrip('/\\'))
    
                    # Build Sonarr payload
                    show_data = {
                        'tvdbId': tvdb_id,
                        'title': show['title'],
                        'qualityProfileId': quality_profile_id,
                        'seasonFolder': True,
                        'rootFolderPath': root_folder,
                        'monitored': monitor_option != 'none',
                        'addOptions': {
                            'searchForMissingEpisodes': search_missing,
                            'monitor': monitor_option if monitor_option != 'firstSeason' else 'none'
                        }
                    }
    
                    if seasons:
                        show_data['seasons'] = seasons
                    elif monitor_option == 'firstSeason':
                        print(f"{YELLOW}Couldn't get season data, monitoring all{RESET}")
                        show_data['addOptions']['monitor'] = 'all'
    
                    if tag_id is not None:
                        show_data['tags'] = [tag_id]
    
                    add_resp = requests.post(f"{sonarr_url}/series", headers=headers, json=show_data)
                    add_resp.raise_for_status()
    
                    if monitor_option != 'none' and search_missing:
                        new_id = add_resp.json()['id']
                        search_cmd = {'name': 'SeriesSearch', 'seriesIds': [new_id]}
                        sr = requests.post(f"{sonarr_url}/command", headers=headers, json=search_cmd)
                        sr.raise_for_status()
                        print(f"Triggered download search for: {show['title']}")
    
                except requests.exceptions.RequestException as e:
                    print(f"{RED}Error adding {show['title']} to Sonarr: {str(e)}{RESET}")
                    if hasattr(e, 'response') and e.response is not None:
                        try:
                            error_details = e.response.json()
                            print(f"{RED}Sonarr error details: {json.dumps(error_details, indent=2)}{RESET}")
                        except:
                            print(f"{RED}Sonarr error response: {e.response.text}{RESET}")
                    continue
    
        except Exception as e:
            print(f"{RED}Error adding shows to Sonarr: {e}{RESET}")
            import traceback
            print(traceback.format_exc())

# ------------------------------------------------------------------------
# OUTPUT FORMATTING
# ------------------------------------------------------------------------
def format_show_output(show: Dict,
                      show_summary: bool = False,
                      index: Optional[int] = None,
                      show_cast: bool = False,
                      show_studio: bool = False,
                      show_language: bool = False,
                      show_imdb_rating: bool = False,
                      show_imdb_link: bool = False) -> str:
    bullet = f"{index}. " if index is not None else "- "
    output = f"{bullet}{CYAN}{show['title']}{RESET} ({show.get('year', 'N/A')})"
    
    if show.get('genres'):
        output += f"\n  {YELLOW}Genres:{RESET} {', '.join(show['genres'])}"

    if show_summary and show.get('summary'):
        output += f"\n  {YELLOW}Summary:{RESET} {show['summary']}"

    if show_cast and show.get('cast'):
        output += f"\n  {YELLOW}Cast:{RESET} {', '.join(show['cast'])}"

    if show_studio and show.get('studio') != "N/A":
        output += f"\n  {YELLOW}Studio:{RESET} {show['studio']}"

    if show_language and show.get('language') != "N/A":
        output += f"\n  {YELLOW}Language:{RESET} {show['language']}"

    if show_imdb_rating and show.get('ratings', {}).get('imdb_rating', 0) > 0:
        rating = show['ratings']['imdb_rating']
        output += f"\n  {YELLOW}IMDb Rating:{RESET} {rating}/10"

    if show_imdb_link and show.get('imdb_id'):
        imdb_link = f"https://www.imdb.com/title/{show['imdb_id']}/"
        output += f"\n  {YELLOW}IMDb Link:{RESET} {imdb_link}"

    return output

# ------------------------------------------------------------------------
# LOGGING / MAIN
# ------------------------------------------------------------------------
ANSI_PATTERN = re.compile(r'\x1b\[[0-9;]*m')

class TeeLogger:
    """
    A simple 'tee' class that writes to both console and a file,
    stripping ANSI color codes for the file.
    """
    def __init__(self, logfile):
        self.logfile = logfile
    
    def write(self, text):
        sys.__stdout__.write(text)
        stripped = ANSI_PATTERN.sub('', text)
        self.logfile.write(stripped)
    
    def flush(self):
        sys.__stdout__.flush()
        self.logfile.flush()

def cleanup_old_logs(log_dir: str, keep_logs: int):
    if keep_logs <= 0:
        return

    all_files = sorted(
        (f for f in os.listdir(log_dir) if f.endswith('.log')),
        key=lambda x: os.path.getmtime(os.path.join(log_dir, x))
    )
    if len(all_files) > keep_logs:
        to_remove = all_files[:len(all_files) - keep_logs]
        for f in to_remove:
            try:
                os.remove(os.path.join(log_dir, f))
            except Exception as e:
                print(f"{YELLOW}Failed to remove old log {f}: {e}{RESET}")

def main():
    start_time = datetime.now()
    print(f"{CYAN}TV Show Recommendations for Plex{RESET}")
    print("-" * 50)
    check_version()
    print("-" * 50)
    
    config_path = os.path.join(os.path.dirname(__file__), 'config.yml')
    
    try:
        with open(config_path, 'r') as f:
            base_config = yaml.safe_load(f)
    except Exception as e:
        print(f"{RED}Could not load config.yml: {e}{RESET}")
        sys.exit(1)

    general = base_config.get('general', {})
    keep_logs = general.get('keep_logs', 0)

    original_stdout = sys.stdout
    log_dir = os.path.join(os.path.dirname(__file__), 'Logs')
    if keep_logs > 0:
        try:
            os.makedirs(log_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file_path = os.path.join(log_dir, f"recommendations_{timestamp}.log")
            lf = open(log_file_path, "w", encoding="utf-8")
            sys.stdout = TeeLogger(lf)

            cleanup_old_logs(log_dir, keep_logs)
        except Exception as e:
            print(f"{RED}Could not set up logging: {e}{RESET}")

    try:
        recommender = PlexTVRecommender(config_path)
        recommendations = recommender.get_recommendations()
        
        print(f"\n{GREEN}=== Recommended Unwatched Shows in Your Library ==={RESET}")
        plex_recs = recommendations.get('plex_recommendations', [])
        if plex_recs:
            for i, show in enumerate(plex_recs, start=1):
                print(format_show_output(
                    show,
                    show_summary=recommender.show_summary,
                    index=i,
                    show_cast=recommender.show_cast,
                    show_studio=recommender.show_studio,
                    show_language=recommender.show_language,
                    show_imdb_rating=recommender.show_imdb_rating,
                    show_imdb_link=recommender.show_imdb_link
                ))
                print()
            recommender.manage_plex_labels(plex_recs)
        else:
            print(f"{YELLOW}No recommendations found in your Plex library matching your criteria.{RESET}")
     
        if not recommender.plex_only:
            print(f"\n{GREEN}=== Recommended Shows to Add to Your Library ==={RESET}")
            trakt_recs = recommendations.get('trakt_recommendations', [])
            if trakt_recs:
                for i, show in enumerate(trakt_recs, start=1):
                    print(format_show_output(
                        show,
                        show_summary=recommender.show_summary,
                        index=i,
                        show_cast=recommender.show_cast,
                        show_studio=recommender.show_studio,
                        show_language=recommender.show_language,
                        show_imdb_rating=recommender.show_imdb_rating,
                        show_imdb_link=recommender.show_imdb_link
                    ))
                    print()
                recommender.add_to_sonarr(trakt_recs)
            else:
                print(f"{YELLOW}No Trakt recommendations found matching your criteria.{RESET}")

    except Exception as e:
        print(f"\n{RED}An error occurred: {e}{RESET}")
        import traceback
        print(traceback.format_exc())

    finally:
        print(f"\n{GREEN}Process completed!{RESET}")
        runtime = datetime.now() - start_time
        hours = runtime.seconds // 3600
        minutes = (runtime.seconds % 3600) // 60
        seconds = runtime.seconds % 60
        print(f"Total runtime: {hours:02d}:{minutes:02d}:{seconds:02d}")

    if keep_logs > 0 and sys.stdout is not original_stdout:
        try:
            sys.stdout.logfile.close()
            sys.stdout = original_stdout
        except Exception as e:
            print(f"{YELLOW}Error closing log file: {e}{RESET}") 
	
if __name__ == "__main__":
    main()