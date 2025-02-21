import os
import plexapi.server
from plexapi.server import PlexServer
from plexapi.myplex import MyPlexAccount
import yaml
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

__version__ = "2.0b07"
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

RATING_MULTIPLIERS = {
    0: 0.1,   # Strong dislike
    1: 0.2,   # Very poor
    2: 0.4,   # Poor
    3: 0.6,   # Below average
    4: 0.8,   # Slightly below average
    5: 1.0,   # Neutral/baseline
    6: 1.2,   # Slightly above average
    7: 1.4,   # Good
    8: 1.6,   # Very good
    9: 1.8,   # Excellent
    10: 2.0   # Outstanding
    }
	
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

class PlexTVRecommender:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.library_title = self.config['plex'].get('TV_library_title', 'TV Shows')
        
        # Initialize counters and caches
        self.cached_watched_count = 0
        self.cached_unwatched_count = 0
        self.cached_library_show_count = 0
        self.watched_data_counters = {}
        self.synced_show_ids = set()
        self.cached_unwatched_shows = []
        self.plex_tmdb_cache = {}
        self.tmdb_keywords_cache = {}
        self.tautulli_watched_rating_keys = set()
        self.watched_show_ids = set()  # New set for tracking all watched shows
        self.users = self._get_configured_users()
    
        print("Initializing recommendation system...")
        if self.config.get('tautulli', {}).get('users'):
            if not self.config['tautulli'].get('url') or not self.config['tautulli'].get('api_key'):
                raise ValueError("Tautulli configuration requires both url and api_key when users are specified")        
        
        print("Connecting to Plex server...")
        self.plex = self._init_plex()
        print(f"Connected to Plex successfully!\n")
        print(f"{YELLOW}Checking Cache...{RESET}")
		
        general_config = self.config.get('general', {})
        self.confirm_operations = general_config.get('confirm_operations', False)
        self.limit_plex_results = general_config.get('limit_plex_results', 10)
        self.limit_trakt_results = general_config.get('limit_trakt_results', 10)
        self.show_summary = general_config.get('show_summary', False)
        self.plex_only = general_config.get('plex_only', False)
        self.show_cast = general_config.get('show_cast', False)
        self.show_language = general_config.get('show_language', False)
        self.show_rating = general_config.get('show_rating', False)
        self.show_imdb_link = general_config.get('show_imdb_link', False)
        
        exclude_genre_str = general_config.get('exclude_genre', '')
        self.exclude_genres = [g.strip().lower() for g in exclude_genre_str.split(',') if g.strip()] if exclude_genre_str else []

        weights_config = self.config.get('weights', {})
        self.weights = {
            'genre_weight': float(weights_config.get('genre_weight', 0.25)),
            'studio_weight': float(weights_config.get('studio_weight', 0.20)),
            'actor_weight': float(weights_config.get('actor_weight', 0.20)),
            'language_weight': float(weights_config.get('language_weight', 0.10)),
            'keyword_weight': float(weights_config.get('keyword_weight', 0.25))
        }

        total_weight = sum(self.weights.values())
        if not abs(total_weight - 1.0) < 1e-6:
            print(f"{YELLOW}Warning: Weights sum to {total_weight}, expected 1.0.{RESET}")
			
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

        # Verify Tautulli/Plex user mapping
        if self.users['tautulli_users']:
            print(f"Validating Tautulli users: {self.users['tautulli_users']}")
            
            # Skip validation completely if 'All' is specified
            if any(u.lower() == 'all' for u in self.users['tautulli_users']):
                print(f"{YELLOW}Using watch history for all Tautulli users{RESET}")
            else:
                # Only validate specific users
                try:
                    test_params = {'apikey': self.config['tautulli']['api_key'], 'cmd': 'get_users'}
                    users_response = requests.get(f"{self.config['tautulli']['url']}/api/v2", params=test_params)
                    if users_response.status_code == 200:
                        tautulli_users = users_response.json()['response']['data']
                        tautulli_usernames = [u['username'] for u in tautulli_users]
                        missing = [u for u in self.users['tautulli_users'] if u not in tautulli_usernames]
                        
                        if missing:
                            # Check for case-insensitive matches
                            for missing_user in missing:
                                close_matches = [t for t in tautulli_usernames 
                                               if t.lower() == missing_user.lower()]
                                if close_matches:
                                    print(f"\n{RED}Error: User '{missing_user}' not found, but found similar username: "
                                          f"'{close_matches[0]}'{RESET}")
                                    print(f"Tautulli usernames are case-sensitive. Please update your config file "
                                          f"to match the exact username.")
                                else:
                                    print(f"\n{RED}Error: User '{missing_user}' not found in Tautulli.{RESET}")
                                    print("Available Tautulli users:")
                                    for username in tautulli_usernames:
                                        print(f"- {username}")
                            raise ValueError("Please check your Tautulli usernames and ensure they match exactly.")
                except requests.exceptions.RequestException as e:
                    raise ValueError(f"Error connecting to Tautulli: {e}")

        # Verify library exists
        if not self.plex.library.section(self.library_title):
            raise ValueError(f"TV Show library '{self.library_title}' not found in Plex")
        
        tmdb_config = self.config.get('TMDB', {})
        self.use_tmdb_keywords = tmdb_config.get('use_TMDB_keywords', False)
        self.tmdb_api_key = tmdb_config.get('api_key', None)

        self.sonarr_config = self.config.get('sonarr', {})

        self.cache_dir = os.path.join(os.path.dirname(__file__), "cache")
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Get user context for cache files
        if self.users['tautulli_users']:
            user_ctx = 'tautulli_' + '_'.join(self.users['tautulli_users'])
        else:
            user_ctx = 'plex_' + '_'.join(self.users['managed_users'])
        safe_ctx = re.sub(r'\W+', '', user_ctx)
        
        # Update cache paths to be user-specific
        self.watched_cache_path = os.path.join(self.cache_dir, f"watched_cache_{safe_ctx}.json")
        self.unwatched_cache_path = os.path.join(self.cache_dir, f"unwatched_cache_{safe_ctx}.json")
        self.trakt_cache_path = os.path.join(self.cache_dir, f"trakt_sync_cache_{safe_ctx}.json")
        self.trakt_sync_cache_path = os.path.join(self.cache_dir, "trakt_sync_cache.json")
         
        # Load watched cache 
        watched_cache = {}
        if os.path.exists(self.watched_cache_path):
            try:
                with open(self.watched_cache_path, 'r', encoding='utf-8') as f:
                    watched_cache = json.load(f)
                    self.cached_watched_count = watched_cache.get('watched_count', 0)
                    self.watched_data_counters = watched_cache.get('watched_data_counters', {})
                    self.plex_tmdb_cache = watched_cache.get('plex_tmdb_cache', {})
                    self.tmdb_keywords_cache = watched_cache.get('tmdb_keywords_cache', {})
                    # Load both watched show tracking mechanisms
                    self.tautulli_watched_rating_keys = set(watched_cache.get('tautulli_watched_rating_keys', []))
                    self.watched_show_ids = set(watched_cache.get('watched_show_ids', []))
                    # Verify TMDB IDs are loaded
                    
            except Exception as e:
                print(f"{YELLOW}Error loading watched cache: {e}{RESET}")
    
        current_library_ids = self._get_library_shows_set()
        
        # Clean up both watched show tracking mechanisms
        self.tautulli_watched_rating_keys = {
            rk for rk in self.tautulli_watched_rating_keys 
            if int(rk) in current_library_ids
        }
        self.watched_show_ids = {
            show_id for show_id in self.watched_show_ids
            if show_id in current_library_ids
        }
		
        # Load unwatched cache
        if os.path.exists(self.unwatched_cache_path):
            try:
                with open(self.unwatched_cache_path, 'r', encoding='utf-8') as f:
                    unwatched_cache = json.load(f)
                    self.cached_library_show_count = unwatched_cache.get('library_show_count', 0)
                    self.cached_unwatched_count = unwatched_cache.get('unwatched_count', 0)
                    self.cached_unwatched_shows = unwatched_cache.get('unwatched_show_details', [])
            except Exception as e:
                print(f"{YELLOW}Error loading unwatched cache: {e}{RESET}") 
				
        if self.plex_tmdb_cache is None:
            self.plex_tmdb_cache = {}
        if self.tmdb_keywords_cache is None:
            self.tmdb_keywords_cache = {}
        if not hasattr(self, 'synced_trakt_history'):
            self.synced_trakt_history = {}

        current_watched_count = self._get_watched_count()
        cache_exists = os.path.exists(self.watched_cache_path)
        
        if (not cache_exists) or (current_watched_count != self.cached_watched_count):
            print("Watched count changed or no cache found; gathering watched data now. This may take a while...\n")
            if self.users['tautulli_users']:
                print("Using Tautulli users for watch history")
                self.watched_data = self._get_tautulli_watched_shows_data()
            else:
                print("Using managed users for watch history")
                self.watched_data = self._get_managed_users_watched_data()
            self.watched_data_counters = self.watched_data
            self.cached_watched_count = current_watched_count
            self._save_watched_cache()
        else:
            print(f"Watched count unchanged. Using cached data for {self.cached_watched_count} shows")
            self.watched_data = self.watched_data_counters
    
        print("Fetching library metadata (for existing Shows checks)...")
        self.library_shows = self._get_library_shows_set()
        self.library_imdb_ids = self._get_library_imdb_ids()
 
    # ------------------------------------------------------------------------
    # CONFIG / SETUP
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

    # ------------------------------------------------------------------------
    # USERS
    # ------------------------------------------------------------------------ 
    def _get_configured_users(self):
        # Get raw managed users list from config
        raw_managed = self.config['plex'].get('managed_users', '')
        managed_users = [u.strip() for u in raw_managed.split(',') if u.strip()]
        
        # Get Tautulli users
        tautulli_users = []
        tautulli_config = self.config.get('tautulli', {})
        if isinstance(tautulli_config.get('users'), list):
            tautulli_users = tautulli_config['users']
        elif isinstance(tautulli_config.get('users'), str):
            tautulli_users = [u.strip() for u in tautulli_config['users'].split(',') if u.strip()]
        
        # Resolve admin account FIRST
        account = MyPlexAccount(token=self.config['plex']['token'])
        admin_user = account.username
        
        # User validation logic
        all_users = account.users()
        all_usernames_lower = {u.title.lower(): u.title for u in all_users}
    
        processed_managed = []
        for user in managed_users:
            user_lower = user.lower()
            if user_lower in ['admin', 'administrator']:
                processed_managed.append(admin_user)
            elif user_lower in all_usernames_lower:
                processed_managed.append(all_usernames_lower[user_lower])
            else:
                print(f"{RED}Error: Managed user '{user}' not found{RESET}")
                raise ValueError(f"User '{user}' not found in Plex account")
    
        # Remove duplicates while preserving order
        seen = set()
        managed_users = [u for u in processed_managed if not (u in seen or seen.add(u))]
        
        # Handle "none" case for Tautulli
        if tautulli_users and tautulli_users[0].lower() == 'none':
            tautulli_users = []
        
        return {
            'managed_users': managed_users,
            'tautulli_users': tautulli_users,
            'admin_user': admin_user
        }

    def _get_current_users(self) -> str:
        if self.users['tautulli_users']:
            return f"Tautulli users: {', '.join(self.users['tautulli_users'])}"
        return f"Managed users: {', '.join(self.users['managed_users'])}"

    def _get_user_specific_connection(self):
        if self.users['tautulli_users']:
            return self.plex
        try:
            account = MyPlexAccount(token=self.config['plex']['token'])
            user = account.user(self.users['managed_users'][0])
            return self.plex.switchUser(user)
        except:
            return self.plex

    def _get_watched_count(self) -> int:
        if self.users['tautulli_users']:
            user_ids = []  # Reuse user ID resolution logic from _get_tautulli_watched_shows_data
            try:
                users_response = requests.get(
                    f"{self.config['tautulli']['url']}/api/v2",
                    params={'apikey': self.config['tautulli']['api_key'], 'cmd': 'get_users'}
                )
                tautulli_users = users_response.json()['response']['data']
                for username in self.users['tautulli_users']:
                    user = next((u for u in tautulli_users 
                               if u['username'].lower() == username.lower()), None)
                    if user:
                        user_ids.append(str(user['user_id']))
            except Exception as e:
                print(f"{YELLOW}Error resolving users: {e}{RESET}")
                return 0
    
            grandparent_keys = set()
            for user_id in user_ids:
                start = 0
                while True:
                    params = {
                        'apikey': self.config['tautulli']['api_key'],
                        'cmd': 'get_history',
                        'media_type': 'episode',
                        'user_id': user_id,
                        'length': 1000,
                        'start': start
                    }
                    response = requests.get(f"{self.config['tautulli']['url']}/api/v2", params=params)
                    data = response.json()['response']['data']
                    
                    if isinstance(data, dict):
                        page_items = data.get('data', [])
                        total_records = data.get('recordsFiltered', 0)
                    else:
                        page_items = data
                        total_records = len(page_items)
                    
                    # Collect unique show rating keys
                    for item in page_items:
                        if item.get('grandparent_rating_key'):
                            grandparent_keys.add(str(item['grandparent_rating_key']))
                    
                    if len(page_items) < params['length'] or start >= total_records:
                        break
                    start += len(page_items)
    
            return len(grandparent_keys)  # Correct count of unique shows
        else:
            # For managed users, sum up all unique watched shows
            try:
                total_watched = set()  # Using set to avoid counting duplicates
                shows_section = self.plex.library.section(self.library_title)
                account = MyPlexAccount(token=self.config['plex']['token'])
                
                users_to_process = self.users['managed_users'] or [self.users['admin_user']]
                
                for username in users_to_process:
                    try:
                        if username.lower() == self.users['admin_user'].lower():
                            user_plex = self.plex
                        else:
                            user = account.user(username)
                            user_plex = self.plex.switchUser(user)
                        
                        watched_shows = user_plex.library.section(self.library_title).search(unwatched=False)
                        total_watched.update(show.ratingKey for show in watched_shows)
                        
                    except Exception as e:
                        print(f"{YELLOW}Error getting watch count for user {username}: {e}{RESET}")
                        continue
                        
                return len(total_watched)
                
            except Exception as e:
                print(f"{YELLOW}Error getting watch count: {e}{RESET}")
                return 0
				
    def _get_tautulli_watched_shows_data(self) -> Dict:
        shows_section = self.plex.library.section(self.library_title)
        counters = {
            'genres': Counter(),
            'studio': Counter(),
            'actors': Counter(),
            'languages': Counter(),
            'tmdb_keywords': Counter(),
            'tmdb_ids': set()
        }
        watched_show_ids = set()
        not_found_count = 0
    
        print(f"{YELLOW}Resolving Tautulli user IDs...{RESET}")
        user_ids = []
        try:
            # Get all Tautulli users
            users_response = requests.get(
                f"{self.config['tautulli']['url']}/api/v2",
                params={
                    'apikey': self.config['tautulli']['api_key'],
                    'cmd': 'get_users'
                }
            )
            users_response.raise_for_status()
            tautulli_users = users_response.json()['response']['data']
    
            # Match configured usernames to user IDs
            for username in self.users['tautulli_users']:
                user = next(
                    (u for u in tautulli_users 
                     if u['username'].lower() == username.lower()),
                    None
                )
                if user:
                    user_ids.append(str(user['user_id']))
                    print(f"Matched '{username}' to ID: {user['user_id']}")
                else:
                    print(f"{RED}User '{username}' not found in Tautulli!{RESET}")
    
        except Exception as e:
            print(f"{RED}Error resolving Tautulli users: {e}{RESET}")
            return self._normalize_all_counters(counters)
    
        if not user_ids:
            print(f"{RED}No valid Tautulli users found!{RESET}")
            return self._normalize_all_counters(counters)
    
        # Fetch history for each user with proper pagination
        history_items = []
        for user_id in user_ids:
            print(f"\n{GREEN}Fetching history for user ID: {user_id}{RESET}")
            start = 0
            total_records = None
    
            while True:
                params = {
                    'apikey': self.config['tautulli']['api_key'],
                    'cmd': 'get_history',
                    'media_type': 'episode',
                    'user_id': user_id,
                    'length': 1000,  # Max per Tautulli API
                    'start': start
                }
    
                try:
                    response = requests.get(
                        f"{self.config['tautulli']['url']}/api/v2",
                        params=params
                    )
                    response.raise_for_status()
                    response_data = response.json()
                    history_data = response_data['response'].get('data', {})
    
                    # Handle different response formats
                    if isinstance(history_data, dict):
                        page_items = history_data.get('data', [])
                        total_records = history_data.get('recordsFiltered', 0)
                    else:  # Legacy format
                        page_items = history_data
                        total_records = len(page_items)
    
                    history_items.extend(page_items)
                    print(f"Fetched {len(page_items)} episodes (Total: {len(history_items)})")
    
                    # Exit conditions
                    if not page_items or len(page_items) == 0:
                        break
                    if start + len(page_items) >= total_records:
                        break
    
                    start += len(page_items)  # Proper pagination increment
    
                except Exception as e:
                    print(f"{RED}Error fetching history page: {e}{RESET}")
                    break
    
        # Process collected history items
        rating_keys = set()
        show_details = {}
    
        # Process history items
        for item in history_items:
            if not isinstance(item, dict):
                continue
                
            rating_key = str(item.get('grandparent_rating_key'))
            if rating_key:
                watched_show_ids.add(int(rating_key))
    
        # Store watched show IDs in class
        self.watched_show_ids.update(watched_show_ids)
        
        # Process shows for recommendation data
        print(f"\nProcessing {len(watched_show_ids)} unique watched shows from Tautulli history:")
        for i, rating_key in enumerate(watched_show_ids, 1):
            self._show_progress("Processing", i, len(watched_show_ids))
            try:
                show = shows_section.fetchItem(rating_key)
                if show:
                    show.reload()
                    self._process_show_counters(show, counters)
                else:
                    not_found_count += 1
            except Exception:
                not_found_count += 1
    
        print(f"{YELLOW}{not_found_count} watched shows not found in library{RESET}")
        return self._normalize_all_counters(counters)
    
    def _is_valid_show_entry(self, entry: dict) -> bool:
        return (
            isinstance(entry, dict) and 
            entry.get('media_type') == 'episode' and
            isinstance(entry.get('metadata'), dict) and
            entry['metadata'].get('title')
        )
    
    def _get_managed_users_watched_data(self):
        counters = {
            'genres': Counter(),
            'studio': Counter(),
            'actors': Counter(),
            'languages': Counter(),
            'tmdb_keywords': Counter(),
            'tmdb_ids': set()
        }
        
        account = MyPlexAccount(token=self.config['plex']['token'])
        admin_user = self.users['admin_user']
        users_to_process = self.users['managed_users'] or [admin_user]
        
        for username in users_to_process:
            try:
                if username.lower() == self.users['admin_user'].lower():
                    user_plex = self.plex
                else:
                    user = account.user(username)
                    user_plex = self.plex.switchUser(user)
                
                tv_sections = [
                    section for section in user_plex.library.sections()
                    if section.type == 'show'
                ]
                if not tv_sections:
                    print(f"{RED}User {username} has no accessible TV libraries.{RESET}")
                    continue
                
                shows_section = tv_sections[0]
                watched_shows = shows_section.search(unwatched=False)
                
                print(f"\nScanning watched shows for {username} in library: {shows_section.title}")
                for i, show in enumerate(watched_shows, 1):
                    self._show_progress(f"Processing {username}'s watched", i, len(watched_shows))
                    self.watched_show_ids.add(show.ratingKey)
                    self._process_show_counters(show, counters)
                    
            except Exception as e:
                print(f"{RED}Error processing user {username}: {e}{RESET}")
                continue
        
        return self._normalize_all_counters(counters)

    # ------------------------------------------------------------------------
    # CACHING LOGIC
    # ------------------------------------------------------------------------
    def _save_watched_cache(self):
        try:
            # Convert TVDB IDs set to list for JSON serialization
            tvdb_ids = list(self.watched_data.get('tvdb_ids', set())) if isinstance(self.watched_data.get('tvdb_ids'), set) else self.watched_data.get('tvdb_ids', [])
            
            # Get watch dates
            watch_dates = self.watched_data.get('watch_dates', {})
            
            cache_data = {
                'watched_count': self.cached_watched_count,
                'watched_data_counters': self.watched_data_counters,
                'plex_tmdb_cache': self.plex_tmdb_cache,
                'tmdb_keywords_cache': self.tmdb_keywords_cache,
                'watched_show_ids': list(self.watched_show_ids),
                'tvdb_ids': tvdb_ids,
                'watch_dates': watch_dates,
                'last_updated': datetime.now().isoformat()
            }
            
            with open(self.watched_cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=4, ensure_ascii=False)
                
        except Exception as e:
            print(f"{YELLOW}Error saving watched cache: {e}{RESET}")

    def _save_unwatched_cache(self):
        try:
            cache_data = {
                'library_show_count': self.cached_library_show_count,
                'unwatched_count': self.cached_unwatched_count,
                'unwatched_show_details': self.cached_unwatched_shows,
                'last_updated': datetime.now().isoformat()
            }
            with open(self.unwatched_cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=4, ensure_ascii=False)
                
        except Exception as e:
            print(f"{YELLOW}Error saving unwatched cache: {e}{RESET}")

    def _save_trakt_sync_cache(self):
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
    def _get_library_shows_set(self) -> Set[tuple]:
        try:
            shows = self.plex.library.section(self.library_title)
            library_shows = set()
            for show in shows.all():
                # Handle both normal titles and titles with embedded years
                title = show.title.lower()
                year = show.year
                
                # Add normal version
                library_shows.add((title, year))
                
                # Check for and strip embedded year pattern
                year_match = re.search(r'\s*\((\d{4})\)$', title)
                if year_match:
                    clean_title = title.replace(year_match.group(0), '').strip()
                    embedded_year = int(year_match.group(1))
                    library_shows.add((clean_title, embedded_year))
                
            return library_shows
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
        if not title:
            return False
            
        title_lower = title.lower()
        
        # Check for year in title and strip it if found
        year_match = re.search(r'\s*\((\d{4})\)$', title_lower)
        if year_match:
            clean_title = title_lower.replace(year_match.group(0), '').strip()
            embedded_year = int(year_match.group(1))
            if (clean_title, embedded_year) in self.library_shows:
                return True
        
        # Check both with and without year
        if (title_lower, year) in self.library_shows:
            return True
            
        # Check title-only matches
        return any(lib_title == title_lower or 
                  lib_title == f"{title_lower} ({year})" or
                  lib_title.replace(f" ({year})", "") == title_lower 
                  for lib_title, lib_year in self.library_shows)

    def _process_show_counters(self, show, counters):
        show_details = self.get_show_details(show)
        
        try:
            rating = float(getattr(show, 'userRating', 0))
        except (TypeError, ValueError):
            try:
                rating = float(getattr(show, 'audienceRating', 5.0))
            except (TypeError, ValueError):
                rating = 5.0
    
        rating = max(0, min(10, int(round(rating))))
        multiplier = RATING_MULTIPLIERS.get(rating, 1.0)
    
        # Process all the existing counters...
        for genre in show_details.get('genres', []):
            counters['genres'][genre] += multiplier
        
        if hasattr(show, 'studio') and show.studio:
            counters['studio'][show.studio.lower()] += multiplier
            
        for actor in show_details.get('cast', [])[:3]:
            counters['actors'][actor] += multiplier
            
        if language := show_details.get('language'):
            counters['languages'][language] += multiplier
            
        for keyword in show_details.get('tmdb_keywords', []):
            counters['tmdb_keywords'][keyword] += multiplier
    
        # Get TVDB IDs and watch dates for all watched episodes
        try:
            watched_episodes = [ep for ep in show.episodes() if ep.isWatched]
            if watched_episodes:
                
                for episode in watched_episodes:
                    episode.reload()
                    
                    # Get watched date and TVDB ID
                    if hasattr(episode, 'lastViewedAt') and hasattr(episode, 'guids'):
                        # Handle lastViewedAt whether it's a timestamp or datetime
                        if isinstance(episode.lastViewedAt, datetime):
                            watched_at = episode.lastViewedAt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                        else:
                            watched_at = datetime.fromtimestamp(int(episode.lastViewedAt)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                        
                        for guid in episode.guids:
                            if 'tvdb://' in guid.id:
                                try:
                                    episode_tvdb_id = int(guid.id.split('tvdb://')[1].split('?')[0])
                                    if 'tvdb_ids' not in counters:
                                        counters['tvdb_ids'] = set()
                                    if 'watch_dates' not in counters:
                                        counters['watch_dates'] = {}
                                    counters['tvdb_ids'].add(episode_tvdb_id)
                                    counters['watch_dates'][episode_tvdb_id] = watched_at
                                    break
                                except (ValueError, IndexError) as e:
                                    print(f"DEBUG: Error parsing TVDB ID for episode of {show.title}: {e}")
                                    continue
        except Exception as e:
            print(f"{YELLOW}Error getting episode TVDB IDs for {show.title}: {e}{RESET}")

    def _normalize_counter(self, counter: Counter) -> Dict[str, float]:
        if not counter:
            return {}
        
        max_value = max(counter.values()) if counter else 1
        return {k: v/max_value for k, v in counter.items()}

    def _normalize_all_counters(self, counters):
        normalized = {
            'genres': self._normalize_counter(counters['genres']),
            'studio': self._normalize_counter(counters['studio']),
            'actors': self._normalize_counter(counters['actors']),
            'languages': self._normalize_counter(counters['languages']),
            'tmdb_keywords': self._normalize_counter(counters['tmdb_keywords'])
        }
        
        if 'tvdb_ids' in counters:
            normalized['tvdb_ids'] = list(counters['tvdb_ids']) if isinstance(counters['tvdb_ids'], set) else counters['tvdb_ids']
        if 'watch_dates' in counters:
            normalized['watch_dates'] = counters['watch_dates']
        
        return normalized

    def _get_library_imdb_ids(self) -> Set[str]:
        imdb_ids = set()
        try:
            shows = self.plex.library.section(self.library_title).all()
            for show in shows:
                if hasattr(show, 'guids'):
                    for guid in show.guids:
                        if guid.id.startswith('imdb://'):
                            imdb_ids.add(guid.id.replace('imdb://', ''))
                            break
        except Exception as e:
            print(f"{YELLOW}Error retrieving IMDb IDs from library: {e}{RESET}")
        return imdb_ids

    def get_show_details(self, show) -> Dict:
        try:
            show.reload()
            
            imdb_id = None
            audience_rating = 0
            tmdb_keywords = []
            
            if hasattr(show, 'guids'):
                for guid in show.guids:
                    if 'imdb://' in guid.id:
                        imdb_id = guid.id.replace('imdb://', '')
                        break
            
            if self.show_rating and hasattr(show, 'ratings'):
                for rating in show.ratings:
                    if (getattr(rating, 'image', '') == 'imdb://image.rating' and 
                        getattr(rating, 'type', '') == 'audience'):
                        try:
                            audience_rating = float(rating.value)
                            break
                        except (ValueError, AttributeError):
                            pass
                            
            if self.use_tmdb_keywords and self.tmdb_api_key:
                tmdb_id = self._get_plex_show_tmdb_id(show)
                if tmdb_id:
                    tmdb_keywords = list(self._get_tmdb_keywords_for_id(tmdb_id))
            
            show_info = {
                'title': show.title,
                'year': getattr(show, 'year', None),
                'genres': self._extract_genres(show),
                'summary': getattr(show, 'summary', ''),
                'studio': getattr(show, 'studio', 'N/A'),
                'language': self._get_show_language(show),
                'imdb_id': imdb_id,
                'ratings': {
                    'audience_rating': audience_rating
                } if audience_rating > 0 else {},
                'cast': [],
                'tmdb_keywords': tmdb_keywords
            }
            
            if self.show_cast and hasattr(show, 'roles'):
                show_info['cast'] = [r.tag for r in show.roles[:3]]
                
            return show_info
                
        except Exception as e:
            print(f"{YELLOW}Error getting show details for {show.title}: {e}{RESET}")
            return {}

    def get_unwatched_library_shows(self) -> List[Dict]:
        print(f"\n{YELLOW}Checking unwatched shows...{RESET}")
        
        # Get current library show count
        shows_section = self.plex.library.section(self.library_title)
        all_shows = shows_section.all()
        current_library_count = len(all_shows)
        
        # Check if we can use cached data
        cache_is_valid = (
            os.path.exists(self.unwatched_cache_path) and
            current_library_count == self.cached_library_show_count and
            len(self.cached_unwatched_shows) == self.cached_unwatched_count
        )
        
        if cache_is_valid:
            print(f"Unwatched count unchanged. Using cached data for {self.cached_unwatched_count} shows")
            return self.cached_unwatched_shows
            
        # If we reach here, we need to rescan
        print(f"Rescanning library for unwatched shows...")
        print(f"Found {current_library_count} shows in Plex library")
    
        # Calculate unwatched shows using cached watched IDs
        unwatched_details = []
        excluded_count = 0
        
        for i, show in enumerate(all_shows, 1):
            self._show_progress("Validating shows", i, current_library_count)
            
            # Skip if show is in watched cache
            if show.ratingKey in self.watched_show_ids:
                continue
                
            try:
                info = self.get_show_details(show)
                
                # Skip genre-excluded shows
                show_genres = info.get('genres', [])
                if any(g in self.exclude_genres for g in show_genres):
                    excluded_count += 1
                    continue
                    
                unwatched_details.append(info)
                
            except Exception as e:
                print(f"{YELLOW}Error processing {show.title}: {e}{RESET}")
                continue
    
        print(f"Found {len(unwatched_details)} unwatched shows")
    
        # Update cache
        self.cached_library_show_count = current_library_count
        self.cached_unwatched_count = len(unwatched_details)
        self.cached_unwatched_shows = unwatched_details
        self._save_unwatched_cache()
    
        return unwatched_details	
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
            plex_show._tmdb_fallback_attempted = True
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
            
            if not episode.media:
                return "N/A"
                
            for media in episode.media:
                for part in media.parts:
                    audio_streams = part.audioStreams()
                    
                    if audio_streams:
                        audio = audio_streams[0]                     
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
    def _authenticate_trakt(self):
        try:
            response = requests.post(
                'https://api.trakt.tv/oauth/device/code',
                headers={'Content-Type': 'application/json'},
                json={
                    'client_id': self.config['trakt']['client_id'],
                    'scope': 'write'
                }
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

    def _clear_trakt_watch_history(self):
        print(f"\n{YELLOW}Clearing Trakt watch history...{RESET}")
        trakt_ids = []
        page = 1
        per_page = 100  # Max allowed by Trakt API
        history_found = False
        
        try:
            while True:
                response = requests.get(
                    "https://api.trakt.tv/sync/history/shows",
                    headers=self.trakt_headers,
                    params={'page': page, 'limit': per_page}
                )
                if response.status_code != 200:
                    print(f"{RED}Error fetching history: {response.status_code}{RESET}")
                    break
                
                data = response.json()
                if not data:
                    break
                
                history_found = True
                for item in data:
                    if 'show' in item and 'ids' in item['show']:
                        trakt_id = item['show']['ids'].get('trakt')
                        if trakt_id:
                            trakt_ids.append(trakt_id)
                
                page += 1
    
            if trakt_ids:
                remove_payload = {
                    "shows": [
                        {"ids": {"trakt": tid}} for tid in trakt_ids
                    ]
                }
                
                remove_response = requests.post(
                    "https://api.trakt.tv/sync/history/remove",
                    headers=self.trakt_headers,
                    json=remove_payload
                )
                
                if remove_response.status_code == 200:
                    deleted = remove_response.json().get('deleted', {}).get('shows', 0)                   
                    # Clear the Trakt sync cache
                    if os.path.exists(self.trakt_sync_cache_path):
                        try:
                            os.remove(self.trakt_sync_cache_path)
                            print(f"{GREEN}Cleared Trakt sync cache.{RESET}")
                        except Exception as e:
                            print(f"{YELLOW}Error removing Trakt sync cache: {e}{RESET}")
                    else:
                        print(f"{GREEN}No Trakt sync cache to clear.{RESET}")
                else:
                    print(f"{RED}Failed to remove history: {remove_response.status_code}{RESET}")
                    print(f"Response: {remove_response.text}")
            elif history_found:
                print(f"{YELLOW}No show IDs found in Trakt history to clear.{RESET}")
            else:
                print(f"{GREEN}No Trakt history found to clear.{RESET}")
                
        except Exception as e:
            print(f"{RED}Error clearing Trakt history: {e}{RESET}")

    def _sync_watched_shows_to_trakt(self):
        if not self.sync_watch_history:
            return
    
        print(f"\n{YELLOW}Starting Trakt watch history sync...{RESET}")
        
        # Load existing synced IDs from cache
        synced_tvdb_ids = set()
        if os.path.exists(self.trakt_sync_cache_path):
            try:
                with open(self.trakt_sync_cache_path, 'r') as f:
                    cache_data = json.load(f)
                    synced_tvdb_ids = set(cache_data.get('synced_tvdb_ids', []))
                    print(f"Loaded {len(synced_tvdb_ids)} previously synced episode IDs from cache")
            except Exception as e:
                print(f"{YELLOW}Error loading Trakt sync cache: {e}{RESET}")
    
        # Get TVDB IDs and watch dates from watched cache
        watched_tvdb_ids = set(self.watched_data.get('tvdb_ids', []))
        watch_dates = self.watched_data.get('watch_dates', {})
        
        if not watched_tvdb_ids:
            print(f"{YELLOW}No watched episodes with TVDB IDs found.{RESET}")
            return
    
        # Find unsynced TVDB IDs
        to_sync = watched_tvdb_ids - synced_tvdb_ids
        if not to_sync:
            print(f"{GREEN}All watched episodes already synced to Trakt.{RESET}")
            return
    
        print(f"Found {len(to_sync)} episodes to sync to Trakt")
        
        # Sync in batches
        batch_size = 100
        newly_synced = set()
        
        for i in range(0, len(to_sync), batch_size):
            batch = list(to_sync)[i:i+batch_size]
            
            # Prepare payload for episodes with actual watch dates
            payload = {
                "episodes": [
                    {
                        "ids": {
                            "tvdb": episode_id
                        },
                        "watched_at": watch_dates.get(episode_id, datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z"))
                    }
                    for episode_id in batch
                ]
            }
    
            print(f"Sending batch {i//batch_size + 1} to Trakt...")
    
            try:
                response = requests.post(
                    "https://api.trakt.tv/sync/history",
                    headers=self.trakt_headers,
                    json=payload
                )
                                
                if response.status_code == 201:
                    response_data = response.json()
                    
                    added_episodes = response_data.get('added', {}).get('episodes', 0)
                    if added_episodes > 0:
                        newly_synced.update(batch)
                        print(f"{GREEN}Successfully synced {added_episodes} episodes{RESET}")
                    else:
                        print(f"{YELLOW}Warning: No episodes were added in this batch{RESET}")
                else:
                    print(f"{RED}Error syncing batch to Trakt: {response.status_code}{RESET}")
                    print(f"Error response: {response.text}")
    
                time.sleep(1)  # Respect rate limiting
    
            except Exception as e:
                print(f"{RED}Error during Trakt sync: {e}{RESET}")
                continue
    
        # Update and save Trakt sync cache
        if newly_synced:
            synced_tvdb_ids.update(newly_synced)
            try:
                with open(self.trakt_sync_cache_path, 'w') as f:
                    json.dump({
                        'synced_tvdb_ids': list(synced_tvdb_ids),
                        'last_sync': datetime.now().isoformat()
                    }, f, indent=4)
                print(f"Successfully synced {len(newly_synced)} episodes to Trakt")
            except Exception as e:
                print(f"{RED}Error saving Trakt sync cache: {e}{RESET}")
    # ------------------------------------------------------------------------
    # CALCULATE SCORES
    # ------------------------------------------------------------------------
    def calculate_show_score(self, show) -> float:
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

    # ------------------------------------------------------------------------
    # GET RECOMMENDATIONS
    # ------------------------------------------------------------------------
    def get_trakt_recommendations(self) -> List[Dict]:
        print(f"\n{YELLOW}Checking Trakt recommendations...{RESET}")
        try:
            # First check if there's any watch history
            history_response = requests.get(
                "https://api.trakt.tv/sync/history/shows",
                headers=self.trakt_headers,
                params={'limit': 1}
            )
            
            if history_response.status_code == 200:
                if not history_response.json():
                    print(f"{YELLOW}No watch history found on Trakt. Skipping recommendations.{RESET}")
                    return []
            else:
                print(f"{RED}Error checking Trakt history: {history_response.status_code}{RESET}")
                return []
    
            # If we have history, proceed with getting recommendations
            print(f"Fetching recommendations from Trakt...")
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
    
                        show_data = s.get('show', {})
                        if not show_data:
                            continue
    
                        base_rating = float(show_data.get('rating', 0.0))
                        show_data['_randomized_rating'] = base_rating + random.uniform(0, 0.5)
    
                    shows.sort(key=lambda x: x.get('show', {}).get('_randomized_rating', 0), reverse=True)
    
                    for s in shows:
                        if len(collected_recs) >= self.limit_trakt_results:
                            break
                
                        show = s.get('show', {})
                        if not show:
                            continue
                
                        title = show.get('title', '').strip()
                        year = show.get('year', None)
                
                        if not title or self._is_show_in_library(title, year):
                            continue
    
                        if self._is_show_in_library(title, year):
                            continue
    
                        ratings = {
                            'audience_rating': round(float(show.get('rating', 0)), 1),
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
            collected_recs.sort(key=lambda x: x.get('ratings', {}).get('audience_rating', 0), reverse=True)
            random.shuffle(collected_recs)
            final_recs = collected_recs[:self.limit_trakt_results]
            return final_recs
    
        except Exception as e:
            print(f"{RED}Error getting Trakt recommendations: {e}{RESET}")
            return []

    def get_recommendations(self) -> Dict[str, List[Dict]]:
        trakt_config = self.config.get('trakt', {})		
        # Check if we need to clear Trakt history first
        if trakt_config.get('clear_watch_history', False):
            self._clear_trakt_watch_history()
        
        # Then proceed with normal sync if enabled
        if self.sync_watch_history:
            self._sync_watched_shows_to_trakt()
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
                        x.get('ratings', {}).get('audience_rating', 0),
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

        print(f"Recommendation process completed!")
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

    # ------------------------------------------------------------------------
    # SONARR
    # ------------------------------------------------------------------------
    def add_to_sonarr(self, recommended_shows: List[Dict]) -> None:
        if not recommended_shows:
            print(f"{YELLOW}No shows to add to Sonarr.{RESET}")
            return
    
        if not self.sonarr_config.get('add_to_sonarr'):
            return
    
        valid_options = ['all', 'none', 'firstSeason']  # Maintaining case sensitivity
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
                if choice in [opt.lower() for opt in valid_options]:
                    monitor_option = next(opt for opt in valid_options if opt.lower() == choice)
                    break
                print(f"{RED}Invalid option. Valid choices: {', '.join(valid_options)}{RESET}")
        else:
            selected_shows = recommended_shows
    
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
                        # Find the existing show
                        existing_show = next(s for s in existing_shows if s['tvdbId'] == tvdb_id)
                        
                        # If monitoring is enabled in config
                        if monitor_option != 'none':
                            print(f"{YELLOW}Show already in Sonarr: {show['title']}{RESET}")
                            print(f"{GREEN}Checking monitoring status...{RESET}")
                            
                            # Get full series data from Sonarr
                            try:
                                series_response = requests.get(
                                    f"{sonarr_url}/series/{existing_show['id']}", 
                                    headers=headers
                                )
                                series_response.raise_for_status()
                                current_series = series_response.json()
                                
                                update_data = current_series.copy()
                                update_data['monitored'] = True
                                
                                # Update season monitoring based on monitor_option
                                if 'seasons' in current_series:
                                    update_data['seasons'] = [
                                        {
                                            'seasonNumber': season['seasonNumber'],
                                            'monitored': (
                                                monitor_option == 'all' or 
                                                (monitor_option == 'firstSeason' and season['seasonNumber'] == 1)
                                            ),
                                            'statistics': season.get('statistics', {}),
                                        }
                                        for season in current_series['seasons']
                                        if season['seasonNumber'] != 0  # Exclude specials
                                    ]
                                    
                                    # Update the show in Sonarr
                                    update_resp = requests.put(
                                        f"{sonarr_url}/series/{existing_show['id']}", 
                                        headers=headers, 
                                        json=update_data
                                    )
                                    update_resp.raise_for_status()
                                    
                                    monitoring_message = 'all seasons' if monitor_option == 'all' else 'first season'
                                    print(f"{GREEN}Updated show and {monitoring_message} monitoring for: {show['title']}{RESET}")
                                    
                                    # If search_missing is enabled, trigger a search
                                    if search_missing:
                                        search_cmd = {
                                            'name': 'MissingEpisodeSearch',
                                            'seriesId': existing_show['id']
                                        }
                                        sr = requests.post(f"{sonarr_url}/command", headers=headers, json=search_cmd)
                                        sr.raise_for_status()
                                        print(f"{GREEN}Triggered search for: {show['title']}{RESET}")
                                        
                            except requests.exceptions.RequestException as e:
                                print(f"{RED}Error updating {show['title']} in Sonarr: {str(e)}{RESET}")
                                if hasattr(e, 'response') and e.response is not None:
                                    try:
                                        error_details = e.response.json()
                                        print(f"{RED}Sonarr error details: {json.dumps(error_details, indent=2)}{RESET}")
                                    except:
                                        print(f"{RED}Sonarr error response: {e.response.text}{RESET}")
                            continue
                        else:
                            print(f"{YELLOW}Already in Sonarr: {show['title']}{RESET}")
                            continue
    
                    # Handle new show addition
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
                        'monitored': True,
                        'addOptions': {
                            'searchForMissingEpisodes': search_missing,
                            'monitor': monitor_option
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
                        print(f"{GREEN}Added and triggered download search for: {show['title']}{RESET}")
                    else:
                        print(f"{GREEN}Added: {show['title']}{RESET}")
    
                except requests.exceptions.RequestException as e:
                    print(f"{RED}Error processing {show['title']}: {str(e)}{RESET}")
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
                      show_language: bool = False,
                      show_rating: bool = False,
                      show_imdb_link: bool = False) -> str:
    bullet = f"{index}. " if index is not None else "- "
    output = f"{bullet}{CYAN}{show['title']}{RESET} ({show.get('year', 'N/A')})"
    
    if show.get('genres'):
        output += f"\n  {YELLOW}Genres:{RESET} {', '.join(show['genres'])}"

    if show_summary and show.get('summary'):
        output += f"\n  {YELLOW}Summary:{RESET} {show['summary']}"

    if show_cast and show.get('cast'):
        output += f"\n  {YELLOW}Cast:{RESET} {', '.join(show['cast'])}"

    if show_language and show.get('language') != "N/A":
        output += f"\n  {YELLOW}Language:{RESET} {show['language']}"

    if show_rating and show.get('ratings', {}).get('audience_rating', 0) > 0:
        rating = show['ratings']['audience_rating']
        output += f"\n  {YELLOW}Rating:{RESET} {rating}/10"

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
    stripping ANSI color codes for the file and handling Unicode characters.
    """
    def __init__(self, logfile):
        self.logfile = logfile
        # Force UTF-8 encoding for stdout
        if hasattr(sys.stdout, 'buffer'):
            self.stdout_buffer = sys.stdout.buffer
        else:
            self.stdout_buffer = sys.stdout
    
    def write(self, text):
        try:
            # Write to console
            if hasattr(sys.stdout, 'buffer'):
                self.stdout_buffer.write(text.encode('utf-8'))
            else:
                sys.__stdout__.write(text)
            
            # Write to file (strip ANSI codes)
            stripped = ANSI_PATTERN.sub('', text)
            self.logfile.write(stripped)
        except UnicodeEncodeError:
            # Fallback for problematic characters
            safe_text = text.encode('ascii', 'replace').decode('ascii')
            if hasattr(sys.stdout, 'buffer'):
                self.stdout_buffer.write(safe_text.encode('utf-8'))
            else:
                sys.__stdout__.write(safe_text)
            stripped = ANSI_PATTERN.sub('', safe_text)
            self.logfile.write(stripped)
    
    def flush(self):
        if hasattr(sys.stdout, 'buffer'):
            self.stdout_buffer.flush()
        else:
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
                    show_language=recommender.show_language,
                    show_rating=recommender.show_rating,
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
                        show_language=recommender.show_language,
                        show_rating=recommender.show_rating,
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