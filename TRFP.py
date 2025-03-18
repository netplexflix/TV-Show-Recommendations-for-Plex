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
import math
import copy

__version__ = "2.1"
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

class ShowCache:
    def __init__(self, cache_dir: str, recommender=None):
        self.all_shows_cache_path = os.path.join(cache_dir, "all_shows_cache.json")
        self.cache = self._load_cache()
        self.recommender = recommender  # Store reference to recommender
        
    def _load_cache(self) -> Dict:
        if os.path.exists(self.all_shows_cache_path):
            try:
                with open(self.all_shows_cache_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"{YELLOW}Error loading all shows cache: {e}{RESET}")
                return {'shows': {}, 'last_updated': None, 'library_count': 0}
        return {'shows': {}, 'last_updated': None, 'library_count': 0}
    
    def update_cache(self, plex, library_title: str, tmdb_api_key: Optional[str] = None):
        shows_section = plex.library.section(library_title)
        all_shows = shows_section.all()
        current_count = len(all_shows)
        
        if current_count == self.cache['library_count']:
            print(f"{GREEN}Show cache is up to date{RESET}")
            return False
            
        print(f"\n{YELLOW}Analyzing library shows...{RESET}")
        
        current_shows = set(str(show.ratingKey) for show in all_shows)
        removed = set(self.cache['shows'].keys()) - current_shows
        
        if removed:
            print(f"{YELLOW}Removing {len(removed)} shows from cache that are no longer in library{RESET}")
            for show_id in removed:
                del self.cache['shows'][show_id]
        
        existing_ids = set(self.cache['shows'].keys())
        new_shows = [show for show in all_shows if str(show.ratingKey) not in existing_ids]
        
        if new_shows:
            print(f"Found {len(new_shows)} new shows to analyze")
            
            for i, show in enumerate(new_shows, 1):
                msg = f"\r{CYAN}Processing show {i}/{len(new_shows)} ({int((i/len(new_shows))*100)}%){RESET}"
                sys.stdout.write(msg)
                sys.stdout.flush()
                
                show_id = str(show.ratingKey)
                try:
                    show.reload()
                    
                    # Add delay between shows
                    if i > 1 and tmdb_api_key:
                        time.sleep(0.5)  # Basic rate limiting
                    
                    imdb_id = None
                    tmdb_id = None
                    if hasattr(show, 'guids'):
                        for guid in show.guids:
                            if 'imdb://' in guid.id:
                                imdb_id = guid.id.replace('imdb://', '')
                            elif 'themoviedb://' in guid.id:
                                try:
                                    tmdb_id = int(guid.id.split('themoviedb://')[1].split('?')[0])
                                except (ValueError, IndexError):
                                    pass
                    
                    # TMDB ID search with retries
                    if not tmdb_id and tmdb_api_key:
                        max_retries = 3
                        for attempt in range(max_retries):
                            try:
                                params = {
                                    'api_key': tmdb_api_key,
                                    'query': show.title,
                                    'first_air_date_year': getattr(show, 'year', None)
                                }
                                resp = requests.get(
                                    "https://api.themoviedb.org/3/search/tv",
                                    params=params,
                                    timeout=15
                                )
                                
                                if resp.status_code == 429:
                                    sleep_time = 2 * (attempt + 1)
                                    print(f"{YELLOW}TMDB rate limit hit, waiting {sleep_time}s...{RESET}")
                                    time.sleep(sleep_time)
                                    continue
                                    
                                if resp.status_code == 200:
                                    results = resp.json().get('results', [])
                                    if results:
                                        tmdb_id = results[0]['id']
                                    break
                                    
                            except (requests.exceptions.ConnectionError, 
                                   requests.exceptions.Timeout,
                                   requests.exceptions.ChunkedEncodingError) as e:
                                print(f"{YELLOW}Connection error, retrying... ({attempt+1}/{max_retries}){RESET}")
                                time.sleep(1)
                                if attempt == max_retries - 1:
                                    print(f"{YELLOW}Failed to get TMDB ID for {show.title} after {max_retries} tries{RESET}")
                            except Exception as e:
                                print(f"{YELLOW}Error getting TMDB ID for {show.title}: {e}{RESET}")
                                break
    
                    tmdb_keywords = []
                    if tmdb_id and tmdb_api_key:
                        max_retries = 3
                        for attempt in range(max_retries):
                            try:
                                kw_resp = requests.get(
                                    f"https://api.themoviedb.org/3/tv/{tmdb_id}/keywords",
                                    params={'api_key': tmdb_api_key},
                                    timeout=15
                                )
                                
                                if kw_resp.status_code == 429:
                                    sleep_time = 2 * (attempt + 1)
                                    print(f"{YELLOW}TMDB rate limit hit, waiting {sleep_time}s...{RESET}")
                                    time.sleep(sleep_time)
                                    continue
                                    
                                if kw_resp.status_code == 200:
                                    keywords = kw_resp.json().get('results', [])
                                    tmdb_keywords = [k['name'].lower() for k in keywords]
                                    break
                                    
                            except (requests.exceptions.ConnectionError,
                                   requests.exceptions.Timeout,
                                   requests.exceptions.ChunkedEncodingError) as e:
                                print(f"{YELLOW}Connection error, retrying... ({attempt+1}/{max_retries}){RESET}")
                                time.sleep(1)
                                if attempt == max_retries - 1:
                                    print(f"{YELLOW}Failed to get keywords for {show.title} after {max_retries} tries{RESET}")
                            except Exception as e:
                                print(f"{YELLOW}Error getting TMDB keywords for {show.title}: {e}{RESET}")
                                break
    
                    # Store in recommender's caches if available
                    if self.recommender and tmdb_id:
                        self.recommender.plex_tmdb_cache[str(show.ratingKey)] = tmdb_id
                        if tmdb_keywords:
                            self.recommender.tmdb_keywords_cache[str(tmdb_id)] = tmdb_keywords
                    
                    show_info = {
                        'title': show.title,
                        'year': getattr(show, 'year', None),
                        'genres': [g.tag.lower() for g in show.genres] if hasattr(show, 'genres') else [],
                        'studio': getattr(show, 'studio', 'N/A'),
                        'cast': [r.tag for r in show.roles[:3]] if hasattr(show, 'roles') else [],
                        'summary': getattr(show, 'summary', ''),
                        'language': self._get_show_language(show),
                        'tmdb_keywords': tmdb_keywords,
                        'tmdb_id': tmdb_id,
                        'imdb_id': imdb_id
                    }
                    self.cache['shows'][show_id] = show_info
                    
                except Exception as e:
                    print(f"{YELLOW}Error processing show {show.title}: {e}{RESET}")
                    continue
                    
        self.cache['library_count'] = current_count
        self.cache['last_updated'] = datetime.now().isoformat()
        self._save_cache()
        print(f"\n{GREEN}Show cache updated{RESET}")
        return True
        
    def _save_cache(self):
        try:
            with open(self.all_shows_cache_path, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"{RED}Error saving all shows cache: {e}{RESET}")

    def _get_show_language(self, show) -> str:
        """Get show's primary audio language from first episode"""
        try:
            episodes = show.episodes()
            if not episodes:
                return "N/A"
    
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
                            
        except Exception as e:
            print(f"DEBUG: Language detection failed: {str(e)}")
        return "N/A"

class PlexTVRecommender:
    def __init__(self, config_path: str, single_user: str = None):
        self.single_user = single_user
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
        self.watched_show_ids = set()
        self.users = self._get_configured_users()
    
        print("Initializing recommendation system...")
        if self.config.get('tautulli', {}).get('users'):
            if not self.config['tautulli'].get('url') or not self.config['tautulli'].get('api_key'):
                raise ValueError("Tautulli configuration requires both url and api_key when users are specified")        
        
        print("Connecting to Plex server...")
        self.plex = self._init_plex()
        print(f"Connected to Plex successfully!\n")
        general_config = self.config.get('general', {})
        self.debug = general_config.get('debug', False)
        print(f"{YELLOW}Checking Cache...{RESET}")	
        tmdb_config = self.config.get('TMDB', {})
        self.use_tmdb_keywords = tmdb_config.get('use_TMDB_keywords', True)
        self.tmdb_api_key = tmdb_config.get('api_key', None)
		
        self.cache_dir = os.path.join(os.path.dirname(__file__), "cache")
        os.makedirs(self.cache_dir, exist_ok=True)
        self.show_cache = ShowCache(self.cache_dir, recommender=self)
        self.show_cache.update_cache(self.plex, self.library_title, self.tmdb_api_key)

        self.confirm_operations = general_config.get('confirm_operations', False)
        self.limit_plex_results = general_config.get('limit_plex_results', 10)
        self.limit_trakt_results = general_config.get('limit_trakt_results', 10)
        self.combine_watch_history = general_config.get('combine_watch_history', True)
        self.randomize_recommendations = general_config.get('randomize_recommendations', True)
        self.normalize_counters = general_config.get('normalize_counters', True)
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
            users_to_validate = [self.single_user] if self.single_user else self.users['tautulli_users']
            print(f"Validating Tautulli user{'s' if not self.single_user else ''}: {users_to_validate}")
            
            if any(u.lower() == 'all' for u in users_to_validate):
                print(f"{YELLOW}Using watch history for all Tautulli users{RESET}")
            else:
                try:
                    test_params = {'apikey': self.config['tautulli']['api_key'], 'cmd': 'get_users'}
                    users_response = requests.get(f"{self.config['tautulli']['url']}/api/v2", params=test_params)
                    if users_response.status_code == 200:
                        tautulli_users = users_response.json()['response']['data']
                        tautulli_usernames = [u['username'] for u in tautulli_users]
                        missing = [u for u in users_to_validate if u not in tautulli_usernames]
                        
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
        


        self.sonarr_config = self.config.get('sonarr', {})
        
        # Get user context for cache files
        if single_user:
            user_ctx = f"plex_{single_user}" if not self.users['tautulli_users'] else f"tautulli_{single_user}"
        else:
            if self.users['tautulli_users']:
                user_ctx = 'tautulli_' + '_'.join(self.users['tautulli_users'])
            else:
                user_ctx = 'plex_' + '_'.join(self.users['managed_users'])
        
        safe_ctx = re.sub(r'\W+', '', user_ctx)
        
        # Update cache paths to be user-specific
        self.watched_cache_path = os.path.join(self.cache_dir, f"watched_cache_{safe_ctx}.json")
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
                    self.plex_tmdb_cache = {str(k): v for k, v in watched_cache.get('plex_tmdb_cache', {}).items()}
                    self.tmdb_keywords_cache = {str(k): v for k, v in watched_cache.get('tmdb_keywords_cache', {}).items()}
                    
                    # Load watched show IDs
                    watched_ids = watched_cache.get('watched_show_ids', [])
                    if isinstance(watched_ids, list):
                        self.watched_show_ids = {int(id_) for id_ in watched_ids if str(id_).isdigit()}
                    else:
                        print(f"{YELLOW}Warning: Invalid watched_show_ids format in cache{RESET}")
                        self.watched_show_ids = set()
                    
                    if not self.watched_show_ids and self.cached_watched_count > 0:
                        print(f"{RED}Warning: Cached watched count is {self.cached_watched_count} but no valid IDs loaded{RESET}")
                        # Force a refresh of watched data
                        self._refresh_watched_data()
                    
            except Exception as e:
                print(f"{YELLOW}Error loading watched cache: {e}{RESET}")
                self._refresh_watched_data()  
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
            # Ensure watched_show_ids are preserved
            if not self.watched_show_ids and 'watched_show_ids' in watched_cache:
                self.watched_show_ids = {int(id_) for id_ in watched_cache['watched_show_ids'] if str(id_).isdigit()}
            if self.debug:
                print(f"DEBUG: Loaded {len(self.watched_show_ids)} watched show IDs from cache")
            
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
        
        # Check if Tautulli users is 'none' or empty
        tautulli_user_config = tautulli_config.get('users')
        if tautulli_user_config and str(tautulli_user_config).lower() != 'none':
            if isinstance(tautulli_user_config, list):
                tautulli_users = tautulli_user_config
            elif isinstance(tautulli_user_config, str):
                tautulli_users = [u.strip() for u in tautulli_user_config.split(',') if u.strip()]
        
        # Resolve admin account
        account = MyPlexAccount(token=self.config['plex']['token'])
        admin_user = account.username
        
        # User validation logic
        all_users = account.users()
        all_usernames_lower = {u.title.lower(): u.title for u in all_users}
        
        processed_managed = []
        for user in managed_users:
            user_lower = user.lower()
            if user_lower in ['admin', 'administrator']:
                # Special case for admin keywords
                processed_managed.append(admin_user)
            elif user_lower == admin_user.lower():
                # Direct match with admin username (case-insensitive)
                processed_managed.append(admin_user)
            elif user_lower in all_usernames_lower:
                # Match with shared users
                processed_managed.append(all_usernames_lower[user_lower])
            else:
                print(f"{RED}Error: Managed user '{user}' not found{RESET}")
                raise ValueError(f"User '{user}' not found in Plex account")
        
        # Remove duplicates while preserving order
        seen = set()
        managed_users = [u for u in processed_managed if not (u in seen or seen.add(u))]
        
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
            user_ids = []
            try:
                users_response = requests.get(
                    f"{self.config['tautulli']['url']}/api/v2",
                    params={'apikey': self.config['tautulli']['api_key'], 'cmd': 'get_users'}
                )
                tautulli_users = users_response.json()['response']['data']
                
                # Only process specified user in single user mode
                users_to_check = [self.single_user] if self.single_user else self.users['tautulli_users']
                
                for username in users_to_check:
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
                    
                    for item in page_items:
                        if item.get('grandparent_rating_key'):
                            grandparent_keys.add(str(item['grandparent_rating_key']))
                    
                    if len(page_items) < params['length'] or start >= total_records:
                        break
                    start += len(page_items)
    
            return len(grandparent_keys)
        else:
            # For managed users
            try:
                total_watched = set()
                shows_section = self.plex.library.section(self.library_title)
                account = MyPlexAccount(token=self.config['plex']['token'])
                
                # Determine which users to process
                if self.single_user:
                    if self.single_user.lower() in ['admin', 'administrator']:
                        users_to_process = [self.users['admin_user']]
                    else:
                        users_to_process = [self.single_user]
                else:
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

    def _get_tautulli_user_ids(self):
        """Resolve configured Tautulli usernames to their user IDs"""
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
    
            # Determine which users to process based on single_user mode
            users_to_match = [self.single_user] if self.single_user else self.users['tautulli_users']
    
            # Match configured usernames to user IDs
            for username in users_to_match:
                user = next(
                    (u for u in tautulli_users 
                     if u['username'].lower() == username.lower()),
                    None
                )
                if user:
                    user_ids.append(str(user['user_id']))
                else:
                    print(f"{RED}User '{username}' not found in Tautulli!{RESET}")
    
        except Exception as e:
            print(f"{RED}Error resolving Tautulli users: {e}{RESET}")
        
        return user_ids
		
    def _get_tautulli_watched_shows_data(self) -> Dict:
        if not self.single_user and hasattr(self, 'watched_data_counters') and self.watched_data_counters:
            return self.watched_data_counters
    
        shows_section = self.plex.library.section(self.library_title)
        counters = {
            'genres': Counter(),
            'studio': Counter(),
            'actors': Counter(),
            'languages': Counter(),
            'tmdb_keywords': Counter(),
            'tmdb_ids': set()  # Initialize as a set for unique IDs
        }
        watched_show_ids = set()
        not_found_count = 0
    
        print(f"{YELLOW}Resolving Tautulli user IDs...{RESET}")
        user_ids = self._get_tautulli_user_ids()
        if not user_ids:
            print(f"{RED}No valid Tautulli users found!{RESET}")
            return counters
    
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
    
        # Process history items
        for item in history_items:
            if not isinstance(item, dict):
                continue
                
            rating_key = str(item.get('grandparent_rating_key'))
            if rating_key:
                watched_show_ids.add(int(rating_key))
    
        # Store watched show IDs in class
        self.watched_show_ids.update(watched_show_ids)
        
        # Use cached show data instead of querying Plex again
        print(f"\nProcessing {len(watched_show_ids)} unique watched shows from Tautulli history:")
        for i, show_id in enumerate(watched_show_ids, 1):
            self._show_progress("Processing", i, len(watched_show_ids))
            
            show_info = self.show_cache.cache['shows'].get(str(show_id))
            if show_info:
                self._process_show_counters_from_cache(show_info, counters)
                
                # Explicitly add TMDB ID to the set if available
                if tmdb_id := show_info.get('tmdb_id'):
                    counters['tmdb_ids'].add(tmdb_id)
            else:
                not_found_count += 1
        
        if self.debug:
            print(f"{YELLOW}{not_found_count} watched shows not found in cache{RESET}")
            print(f"{GREEN}Collected {len(counters['tmdb_ids'])} unique TMDB IDs{RESET}")
        
        return counters
    
    def _get_managed_users_watched_data(self):
        # Return cached data if available and we're not in single user mode
        if not self.single_user and hasattr(self, 'watched_data_counters') and self.watched_data_counters:
            if self.debug:
                print("DEBUG: Using cached watched data")
            return self.watched_data_counters
    
        # Only proceed with scanning if we need to
        if hasattr(self, 'watched_data_counters') and self.watched_data_counters:
            if self.debug:
                print("DEBUG: Using existing watched data")
            return self.watched_data_counters
    
        counters = {
            'genres': Counter(),
            'studio': Counter(),
            'actors': Counter(),
            'languages': Counter(),
            'tmdb_keywords': Counter(),
            'tmdb_ids': set()  # Initialize as a set for unique IDs
        }
        
        account = MyPlexAccount(token=self.config['plex']['token'])
        admin_user = self.users['admin_user']
        
        # Determine which users to process
        if self.single_user:
            # Check if the single user is the admin
            if self.single_user.lower() in ['admin', 'administrator']:
                users_to_process = [admin_user]
            else:
                users_to_process = [self.single_user]
        else:
            users_to_process = self.users['managed_users'] or [admin_user]
        
        for username in users_to_process:
            try:
                # Check if current user is admin (using case-insensitive comparison)
                if username.lower() == admin_user.lower():
                    user_plex = self.plex
                else:
                    user = account.user(username)
                    user_plex = self.plex.switchUser(user)
                
                watched_shows = user_plex.library.section(self.library_title).search(unwatched=False)
                
                print(f"\nScanning watched shows for {username}")
                for i, show in enumerate(watched_shows, 1):
                    self._show_progress(f"Processing {username}'s watched", i, len(watched_shows))
                    self.watched_show_ids.add(int(show.ratingKey))
                    
                    show_info = self.show_cache.cache['shows'].get(str(show.ratingKey))
                    if show_info:
                        self._process_show_counters_from_cache(show_info, counters)
                        
                        # Explicitly add TMDB ID to the set if available
                        if tmdb_id := show_info.get('tmdb_id'):
                            counters['tmdb_ids'].add(tmdb_id)
                    
            except Exception as e:
                print(f"{RED}Error processing user {username}: {e}{RESET}")
                continue
        
        if self.debug:
            print(f"{GREEN}Collected {len(counters['tmdb_ids'])} unique TMDB IDs{RESET}")
        
        return counters

    # ------------------------------------------------------------------------
    # CACHING LOGIC
    # ------------------------------------------------------------------------
    def _save_watched_cache(self):
        try:
            if self.debug:
                print(f"DEBUG: Saving cache with {len(self.plex_tmdb_cache)} TMDB IDs and {len(self.tmdb_keywords_cache)} keyword sets")
            
            # Create a copy of the watched data to modify for serialization
            watched_data_for_cache = copy.deepcopy(self.watched_data_counters)
            
            # Convert any set objects to lists for JSON serialization
            if 'tmdb_ids' in watched_data_for_cache and isinstance(watched_data_for_cache['tmdb_ids'], set):
                watched_data_for_cache['tmdb_ids'] = list(watched_data_for_cache['tmdb_ids'])
            
            cache_data = {
                'watched_count': self.cached_watched_count,
                'watched_data_counters': watched_data_for_cache,
                'plex_tmdb_cache': {str(k): v for k, v in self.plex_tmdb_cache.items()},
                'tmdb_keywords_cache': {str(k): v for k, v in self.tmdb_keywords_cache.items()},
                'watched_show_ids': list(self.watched_show_ids),
                'last_updated': datetime.now().isoformat()
            }
            
            with open(self.watched_cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=4, ensure_ascii=False)
                
            if self.debug:
                print(f"DEBUG: Cache saved successfully")
                
        except Exception as e:
            print(f"{YELLOW}Error saving watched cache: {e}{RESET}")

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

    def _process_show_counters_from_cache(self, show_info: Dict, counters: Dict) -> None:
        try:
            rating = float(show_info.get('user_rating', 0))
            if not rating:
                rating = float(show_info.get('audience_rating', 5.0))
            rating = max(0, min(10, int(round(rating))))
            multiplier = RATING_MULTIPLIERS.get(rating, 1.0)
    
            # Process all counters using cached data
            for genre in show_info.get('genres', []):
                counters['genres'][genre] += multiplier
            
            if studio := show_info.get('studio'):
                counters['studio'][studio.lower()] += multiplier
                
            for actor in show_info.get('cast', [])[:3]:
                counters['actors'][actor] += multiplier
                
            if language := show_info.get('language'):
                counters['languages'][language.lower()] += multiplier
                
            # Store TMDB data in caches if available
            if tmdb_id := show_info.get('tmdb_id'):
                # Using the show_id from the cache key instead of ratingKey
                show_id = next((k for k, v in self.show_cache.cache['shows'].items() 
                              if v.get('title') == show_info['title'] and 
                              v.get('year') == show_info.get('year')), None)
                if show_id:
                    self.plex_tmdb_cache[str(show_id)] = tmdb_id
                    if keywords := show_info.get('tmdb_keywords', []):
                        self.tmdb_keywords_cache[str(tmdb_id)] = keywords
                        counters['tmdb_keywords'].update({k: multiplier for k in keywords})
    
        except Exception as e:
            print(f"{YELLOW}Error processing counters for {show_info.get('title')}: {e}{RESET}")
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
		
    def _validate_watched_shows(self):
        cleaned_ids = set()
        for show_id in self.watched_show_ids:
            try:
                cleaned_ids.add(int(str(show_id)))
            except (ValueError, TypeError):
                print(f"{YELLOW}Invalid watched show ID found: {show_id}{RESET}")
        self.watched_show_ids = cleaned_ids

    def _refresh_watched_data(self):
        """Force refresh of watched data"""
        if self.users['tautulli_users']:
            self.watched_data = self._get_tautulli_watched_shows_data()
        else:
            self.watched_data = self._get_managed_users_watched_data()
        self.watched_data_counters = self.watched_data
        self._save_watched_cache()
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
            return None

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
        if tmdb_id:
            if self.debug:
                print(f"DEBUG: Adding TMDB ID {tmdb_id} to cache for {plex_show.title}")
            self.plex_tmdb_cache[str(plex_show.ratingKey)] = tmdb_id
            self._save_watched_cache()
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

        if kw_set:
            if self.debug:
                print(f"DEBUG: Adding {len(kw_set)} keywords to cache for TMDB ID {tmdb_id}")
            self.tmdb_keywords_cache[str(tmdb_id)] = list(kw_set)  # Convert key to string
            self._save_watched_cache()
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
        
        # Load existing synced episode IDs from cache
        previously_synced_ids = set()
        if os.path.exists(self.trakt_sync_cache_path):
            try:
                with open(self.trakt_sync_cache_path, 'r') as f:
                    cache_data = json.load(f)
                    if 'synced_episode_ids' in cache_data:
                        previously_synced_ids = set(int(id) for id in cache_data['synced_episode_ids'] if str(id).isdigit())
                        print(f"Loaded previously synced episode IDs from cache")
            except Exception as e:
                print(f"{YELLOW}Error loading Trakt sync cache: {e}{RESET}")
        
        watched_episodes = []
        
        try:
            if self.users['tautulli_users']:
                # Get Tautulli history for watched episodes
                user_ids = self._get_tautulli_user_ids()
                
                # First, get all watch history from Tautulli
                all_history_items = []
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
                        
                        try:
                            response = requests.get(
                                f"{self.config['tautulli']['url']}/api/v2", 
                                params=params,
                                timeout=30
                            )
                            
                            response.raise_for_status()
                            data = response.json()['response']['data']
                            
                            if isinstance(data, dict):
                                history_items = data.get('data', [])
                            else:
                                history_items = data
                            
                            all_history_items.extend(history_items)
                            
                            # Check if we should continue to next page
                            if len(history_items) < 1000:
                                break
                                
                            start += len(history_items)
                            
                        except Exception as e:
                            if self.debug:
                                print(f"DEBUG: Error fetching Tautulli history: {e}")
                            break
                
                print(f"Gathering episode data from {len(all_history_items)} history items...")
                
                # Group episodes by rating_key (episode ID)
                episode_groups = {}
                for item in all_history_items:
                    if item.get('watched_status') == 1:
                        key = item['rating_key']
                        if key not in episode_groups:
                            episode_groups[key] = item
                        
                # Process each unique episode with progress indicator
                total_episodes = len(episode_groups)
                for i, (key, item) in enumerate(episode_groups.items()):
                    # Show progress every 10 episodes or for the first/last one
                    if i == 0 or i == total_episodes-1 or (i+1) % 10 == 0:
                        progress = int((i+1) / total_episodes * 100)
                        sys.stdout.write(f"\rProcessing episodes: {i+1}/{total_episodes} ({progress}%)")
                        sys.stdout.flush()
                        
                    try:
                        # Get the actual episode from Plex to ensure correct TVDB ID
                        episode = self.plex.fetchItem(int(key))
                        
                        # Extract TVDB ID directly from the episode's GUIDs
                        tvdb_id = None
                        if hasattr(episode, 'guids'):
                            for guid in episode.guids:
                                if 'tvdb://' in guid.id:
                                    try:
                                        # Extract the actual episode TVDB ID
                                        tvdb_id = int(guid.id.split('tvdb://')[1].split('?')[0])
                                        break
                                    except (ValueError, IndexError):
                                        continue
                        
                        if not tvdb_id:
                            continue
                        
                        # Convert timestamp
                        timestamp = int(item['date'])
                        watched_date = datetime.fromtimestamp(timestamp)
                        trakt_date = watched_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                        
                        watched_episodes.append({
                            'tvdb_id': tvdb_id,
                            'show_title': item['grandparent_title'],
                            'season': item['parent_media_index'],
                            'episode': item['media_index'],
                            'watched_at': trakt_date
                        })
                            
                    except Exception as e:
                        if self.debug:
                            print(f"\nDEBUG: Error processing episode {item.get('full_title', 'Unknown')}: {e}")
                        continue
                
                # Print newline after progress indicator
                print("")
                
            else:
                # Process each show's episodes with progress
                print(f"Gathering episode data from {len(self.watched_show_ids)} watched shows...")
                total_shows = len(self.watched_show_ids)
                show_count = 0
                episode_count = 0
                
                for show_id in self.watched_show_ids:
                    show_count += 1
                    try:
                        show = self.plex.fetchItem(show_id)
                        show_episodes = [ep for ep in show.episodes() if ep.isWatched]
                        
                        # Update progress
                        progress = int(show_count / total_shows * 100)
                        sys.stdout.write(f"\rProcessing shows: {show_count}/{total_shows} ({progress}%) - Found {episode_count} episodes")
                        sys.stdout.flush()
                        
                        for episode in show_episodes:
                            if hasattr(episode, 'guids'):
                                # Extract TVDB ID directly from the episode's GUIDs
                                tvdb_id = None
                                for guid in episode.guids:
                                    if 'tvdb://' in guid.id:
                                        try:
                                            tvdb_id = int(guid.id.split('tvdb://')[1].split('?')[0])
                                            break
                                        except (ValueError, IndexError):
                                            continue
                                
                                if not tvdb_id:
                                    continue
                                    
                                watched_at = None
                                if hasattr(episode, 'lastViewedAt'):
                                    if isinstance(episode.lastViewedAt, datetime):
                                        watched_at = episode.lastViewedAt
                                    else:
                                        watched_at = datetime.fromtimestamp(int(episode.lastViewedAt))
                                
                                if not watched_at:
                                    continue
                                    
                                watched_episodes.append({
                                    'tvdb_id': tvdb_id,
                                    'show_title': show.title,
                                    'season': episode.seasonNumber,
                                    'episode': episode.index,
                                    'watched_at': watched_at.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                                })
                                episode_count += 1
                                
                    except Exception as e:
                        if self.debug:
                            print(f"\nDEBUG: Error processing show {show_id}: {e}")
                        continue
                
                # Print newline after progress indicator
                print("")
        
            if not watched_episodes:
                print(f"{YELLOW}No episodes found to sync to Trakt{RESET}")
                return
            
            # Filter out already synced episodes
            new_episodes = []
            for episode in watched_episodes:
                if episode['tvdb_id'] not in previously_synced_ids:
                    new_episodes.append(episode)
            
            if not new_episodes:
                print(f"{GREEN}All episodes already synced to Trakt{RESET}")
                return
            
            print(f"Found {len(new_episodes)} new episodes to sync (out of {len(watched_episodes)} total)")
            
            # Sync only new episodes in batches
            batch_size = 100
            newly_synced = set()
            batch_count = 0
            total_batches = math.ceil(len(new_episodes) / batch_size)
            
            for i in range(0, len(new_episodes), batch_size):
                batch_count += 1
                batch = new_episodes[i:i+batch_size]
                                
                payload = {
                    "episodes": [
                        {
                            "ids": {
                                "tvdb": ep['tvdb_id']
                            },
                            "watched_at": ep['watched_at']
                        }
                        for ep in batch
                    ]
                }
        
                try:
                    response = requests.post(
                        "https://api.trakt.tv/sync/history",
                        headers=self.trakt_headers,
                        json=payload,
                        timeout=60
                    )
                                    
                    if response.status_code == 201:
                        response_data = response.json()
                        added_episodes = response_data.get('added', {}).get('episodes', 0)
                        if added_episodes > 0:
                            newly_synced.update(ep['tvdb_id'] for ep in batch)
                            print(f"{GREEN}Successfully synced {added_episodes} episodes{RESET}")
                        else:
                            print(f"{YELLOW}Warning: No episodes were added in this batch{RESET}")
                    else:
                        print(f"{RED}Error syncing batch to Trakt: {response.status_code}{RESET}")
                        print(f"Error response: {response.text}")
        
                    time.sleep(2)  # Respect rate limiting
        
                except Exception as e:
                    print(f"{RED}Error during Trakt sync: {e}{RESET}")
                    time.sleep(2)
                    continue
        
            # Update and save Trakt sync cache - combine previously synced with newly synced
            if newly_synced:
                try:
                    all_synced = previously_synced_ids.union(newly_synced)
                    with open(self.trakt_sync_cache_path, 'w') as f:
                        json.dump({
                            'synced_episode_ids': list(all_synced),
                            'last_sync': datetime.now().isoformat()
                        }, f, indent=4)
                except Exception as e:
                    print(f"{RED}Error saving Trakt sync cache: {e}{RESET}")
        except Exception as outer_e:
            print(f"{RED}Unexpected error during Trakt sync process: {outer_e}{RESET}")
            if self.debug:
                import traceback
                print(f"DEBUG: {traceback.format_exc()}")
    # ------------------------------------------------------------------------
    # CALCULATE SCORES
    # ------------------------------------------------------------------------
    def _calculate_similarity_from_cache(self, show_info: Dict) -> Tuple[float, Dict]:
        """Calculate similarity score using cached show data and return score with breakdown"""
        try:
            score = 0.0
            score_breakdown = {
                'genre_score': 0.0,
                'studio_score': 0.0,
                'actor_score': 0.0,
                'language_score': 0.0,
                'keyword_score': 0.0,
                'details': {
                    'genres': [],
                    'studio': None,
                    'actors': [],
                    'language': None,
                    'keywords': []
                }
            }
            
            weights = self.weights
            user_prefs = {
                'genres': Counter(self.watched_data.get('genres', {})),
                'studio': Counter(self.watched_data.get('studio', {})),
                'actors': Counter(self.watched_data.get('actors', {})),
                'languages': Counter(self.watched_data.get('languages', {})),
                'keywords': Counter(self.watched_data.get('tmdb_keywords', {}))
            }
            
            max_counts = {
                'genres': max(user_prefs['genres'].values()) if user_prefs['genres'] else 1,
                'studio': max(user_prefs['studio'].values()) if user_prefs['studio'] else 1,
                'actors': max(user_prefs['actors'].values()) if user_prefs['actors'] else 1,
                'languages': max(user_prefs['languages'].values()) if user_prefs['languages'] else 1,
                'keywords': max(user_prefs['keywords'].values()) if user_prefs['keywords'] else 1
            }
    
            # Genre Score
            show_genres = set(show_info.get('genres', []))
            if show_genres:
                genre_scores = []
                for genre in show_genres:
                    genre_count = user_prefs['genres'].get(genre, 0)
                    if genre_count > 0:
                        if self.normalize_counters:
                            # Enhanced normalization with square root to strengthen effect
                            # This will boost lower values more significantly
                            normalized_score = math.sqrt(genre_count / max_counts['genres'])
                            genre_scores.append(normalized_score)
                            score_breakdown['details']['genres'].append(
                                f"{genre} (count: {genre_count}, norm: {round(normalized_score, 2)})"
                            )
                        else:
                            # When not normalizing, use raw relative proportion
                            normalized_score = min(genre_count / max_counts['genres'], 1.0)
                            genre_scores.append(normalized_score)
                            score_breakdown['details']['genres'].append(
                                f"{genre} (count: {genre_count}, norm: {round(normalized_score, 2)})"
                            )
                if genre_scores:
                    genre_final = (sum(genre_scores) / len(genre_scores)) * weights.get('genre_weight', 0.25)
                    score += genre_final
                    score_breakdown['genre_score'] = round(genre_final, 3)
    
            # Studio Score
            if show_info.get('studio') and show_info['studio'] != 'N/A':
                studio_count = user_prefs['studio'].get(show_info['studio'].lower(), 0)
                if studio_count > 0:
                    if self.normalize_counters:
                        normalized_score = math.sqrt(studio_count / max_counts['studio'])
                    else:
                        normalized_score = min(studio_count / max_counts['studio'], 1.0)
                    
                    studio_final = normalized_score * weights.get('studio_weight', 0.20)
                    score += studio_final
                    score_breakdown['studio_score'] = round(studio_final, 3)
                    score_breakdown['details']['studio'] = f"{show_info['studio']} (count: {studio_count}, norm: {round(normalized_score, 2)})"
    
            # Actor Score
            show_cast = show_info.get('cast', [])
            if show_cast:
                actor_scores = []
                matched_actors = 0
                for actor in show_cast:
                    actor_count = user_prefs['actors'].get(actor, 0)
                    if actor_count > 0:
                        matched_actors += 1
                        if self.normalize_counters:
                            normalized_score = math.sqrt(actor_count / max_counts['actors'])
                        else:
                            normalized_score = min(actor_count / max_counts['actors'], 1.0)
                            
                        actor_scores.append(normalized_score)
                        score_breakdown['details']['actors'].append(
                            f"{actor} (count: {actor_count}, norm: {round(normalized_score, 2)})"
                        )
                if matched_actors > 0:
                    actor_score = sum(actor_scores) / matched_actors
                    if matched_actors > 3:
                        actor_score *= (3 / matched_actors)  # Normalize if many matches
                    actor_final = actor_score * weights.get('actor_weight', 0.20)
                    score += actor_final
                    score_breakdown['actor_score'] = round(actor_final, 3)
    
            # Language Score
            show_language = show_info.get('language', 'N/A')
            if show_language != 'N/A':
                show_lang_lower = show_language.lower()
                            
                lang_count = user_prefs['languages'].get(show_lang_lower, 0)
                
                if lang_count > 0:
                    if self.normalize_counters:
                        normalized_score = math.sqrt(lang_count / max_counts['languages'])
                    else:
                        normalized_score = min(lang_count / max_counts['languages'], 1.0)
                    
                    lang_final = normalized_score * weights.get('language_weight', 0.10)
                    score += lang_final
                    score_breakdown['language_score'] = round(lang_final, 3)
                    score_breakdown['details']['language'] = f"{show_language} (count: {lang_count}, norm: {round(normalized_score, 2)})"
    
            # TMDB Keywords Score
            if self.use_tmdb_keywords and show_info.get('tmdb_keywords'):
                keyword_scores = []
                for kw in show_info['tmdb_keywords']:
                    count = user_prefs['keywords'].get(kw, 0)
                    if count > 0:
                        if self.normalize_counters:
                            normalized_score = math.sqrt(count / max_counts['keywords'])
                        else:
                            normalized_score = min(count / max_counts['keywords'], 1.0)
                            
                        keyword_scores.append(normalized_score)
                        score_breakdown['details']['keywords'].append(
                            f"{kw} (count: {count}, norm: {round(normalized_score, 2)})"
                        )
                if keyword_scores:
                    keyword_final = (sum(keyword_scores) / len(keyword_scores)) * weights.get('keyword_weight', 0.25)
                    score += keyword_final
                    score_breakdown['keyword_score'] = round(keyword_final, 3)
    
            # Ensure final score doesn't exceed 1.0 (100%)
            score = min(score, 1.0)
    
            return score, score_breakdown
    
        except Exception as e:
            print(f"{YELLOW}Error calculating similarity score for {show_info.get('title', 'Unknown')}: {e}{RESET}")
            return 0.0, score_breakdown

    def _print_similarity_breakdown(self, show_info: Dict, score: float, breakdown: Dict):
        """Print detailed breakdown of similarity score calculation"""
        print(f"\n{CYAN}Similarity Score Breakdown for '{show_info['title']}'{RESET}")
        print(f"Total Score: {round(score * 100, 1)}%")
        print(f"├─ Genre Score: {round(breakdown['genre_score'] * 100, 1)}%")
        if breakdown['details']['genres']:
            print(f"│  └─ Matching genres: {', '.join(breakdown['details']['genres'])}")
        print(f"├─ Studio Score: {round(breakdown['studio_score'] * 100, 1)}%")
        if breakdown['details']['studio']:
            print(f"│  └─ Studio match: {breakdown['details']['studio']}")
        print(f"├─ Actor Score: {round(breakdown['actor_score'] * 100, 1)}%")
        if breakdown['details']['actors']:
            print(f"│  └─ Matching actors: {', '.join(breakdown['details']['actors'])}")
        print(f"├─ Language Score: {round(breakdown['language_score'] * 100, 1)}%")
        if breakdown['details']['language']:
            print(f"│  └─ Language match: {breakdown['details']['language']}")
        print(f"└─ Keyword Score: {round(breakdown['keyword_score'] * 100, 1)}%")
        if breakdown['details']['keywords']:
            print(f"   └─ Matching keywords: {', '.join(breakdown['details']['keywords'])}")
        print("")
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
        if self.cached_watched_count > 0 and not self.watched_show_ids:
            # Force refresh of watched data
            if self.users['tautulli_users']:
                self.watched_data = self._get_tautulli_watched_shows_data()
            else:
                self.watched_data = self._get_managed_users_watched_data()
            self.watched_data_counters = self.watched_data
            self._save_watched_cache()
        
        trakt_config = self.config.get('trakt', {})        
        
        # Handle Trakt operations if configured AND plex_only is not enabled
        if not self.plex_only:
            if trakt_config.get('clear_watch_history', False):
                self._clear_trakt_watch_history()
            if self.sync_watch_history:
                self._sync_watched_shows_to_trakt()
                self._save_cache()
    
        # Get all shows from cache
        all_shows = self.show_cache.cache['shows']
        
        print(f"\n{YELLOW}Processing recommendations...{RESET}")
        
        # Filter out watched shows and excluded genres
        unwatched_shows = []
        excluded_count = 0
        
        for show_id, show_info in all_shows.items():
            # Skip if show is watched
            if int(str(show_id)) in self.watched_show_ids:
                continue
                
            # Skip if show has excluded genres
            if any(g in self.exclude_genres for g in show_info.get('genres', [])):
                excluded_count += 1
                continue
                
            unwatched_shows.append(show_info)
    
        if excluded_count > 0:
            print(f"Excluded {excluded_count} shows based on genre filters")
    
        if not unwatched_shows:
            print(f"{YELLOW}No unwatched shows found matching your criteria.{RESET}")
            plex_recs = []
        else:
            print(f"Calculating similarity scores for {len(unwatched_shows)} shows...")
            
            # Calculate similarity scores
            scored_shows = []
            for i, show_info in enumerate(unwatched_shows, 1):
                self._show_progress("Processing", i, len(unwatched_shows))
                try:
                    similarity_score, breakdown = self._calculate_similarity_from_cache(show_info)
                    show_info['similarity_score'] = similarity_score
                    show_info['score_breakdown'] = breakdown
                    scored_shows.append(show_info)
                except Exception as e:
                    print(f"{YELLOW}Error processing {show_info['title']}: {e}{RESET}")
                    continue
            
            # Sort by similarity score
            scored_shows.sort(key=lambda x: x['similarity_score'], reverse=True)
            
            if self.randomize_recommendations:
                # Take top 10% of shows by similarity score and randomize
                top_count = max(int(len(scored_shows) * 0.1), self.limit_plex_results)
                top_pool = scored_shows[:top_count]
                plex_recs = random.sample(top_pool, min(self.limit_plex_results, len(top_pool)))
            else:
                # Take top shows directly by similarity score
                plex_recs = scored_shows[:self.limit_plex_results]
            
            # Print detailed breakdowns for final recommendations if debug is enabled
            if self.debug:
                print(f"\n{GREEN}=== Similarity Score Breakdowns for Recommendations ==={RESET}")
                for show in plex_recs:
                    self._print_similarity_breakdown(show, show['similarity_score'], show['score_breakdown'])
    
        # Get Trakt recommendations if enabled
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
    
            # Handle username appending for labels
            if self.config['plex'].get('append_usernames', False):
                if self.single_user:
                    # For single user mode, only append the current user
                    user_suffix = re.sub(r'\W+', '_', self.single_user.strip())
                    label_name = f"{label_name}_{user_suffix}"
                else:
                    # For combined mode, append all users
                    users = []
                    if self.users['tautulli_users']:
                        users = self.users['tautulli_users']
                    else:
                        users = self.users['managed_users']
                    
                    if users:
                        sanitized_users = [re.sub(r'\W+', '_', user.strip()) for user in users]
                        user_suffix = '_'.join(sanitized_users)
                        label_name = f"{label_name}_{user_suffix}"
    
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
    
        valid_options = ['all', 'none', 'firstSeason']
        monitor_option = self.sonarr_config.get('monitor_option', 'all')
        search_missing = self.sonarr_config.get('search_missing', False)
        season_folder = self.sonarr_config.get('seasonFolder', True)
        
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
                # Get the base tag name
                tag_name = self.sonarr_config['sonarr_tag']
                
                # Handle username appending for Sonarr tags if enabled
                if self.sonarr_config.get('append_usernames', False):
                    if self.single_user:
                        # For single user mode, only append the current user
                        user_suffix = re.sub(r'\W+', '_', self.single_user.strip())
                        tag_name = f"{tag_name}_{user_suffix}"
                    else:
                        # For combined mode, append all users
                        users = []
                        if self.users['tautulli_users']:
                            users = self.users['tautulli_users']
                        else:
                            users = self.users['managed_users']
                        
                        if users:
                            sanitized_users = [re.sub(r'\W+', '_', user.strip()) for user in users]
                            user_suffix = '_'.join(sanitized_users)
                            tag_name = f"{tag_name}_{user_suffix}"
                
                # Get or create the tag in Sonarr
                tags_response = requests.get(f"{sonarr_url}/tag", headers=headers)
                tags_response.raise_for_status()
                tags = tags_response.json()
                tag = next((t for t in tags if t['label'].lower() == tag_name.lower()), None)
                if tag:
                    tag_id = tag['id']
                else:
                    tag_response = requests.post(
                        f"{sonarr_url}/tag",
                        headers=headers,
                        json={'label': tag_name}
                    )
                    tag_response.raise_for_status()
                    tag_id = tag_response.json()['id']
                    print(f"{GREEN}Created new Sonarr tag: {tag_name}{RESET}")
    
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
                        
                        # Get the full series data from Sonarr regardless of monitoring option
                        try:
                            series_response = requests.get(
                                f"{sonarr_url}/series/{existing_show['id']}", 
                                headers=headers
                            )
                            series_response.raise_for_status()
                            current_series = series_response.json()
                            
                            # Check if we need to update tags
                            needs_tag_update = tag_id is not None and tag_id not in current_series.get('tags', [])
                            
                            # If monitoring is enabled or we need to add a tag
                            if monitor_option != 'none' or needs_tag_update:
                                print(f"{YELLOW}Show already in Sonarr: {show['title']}{RESET}")
                                
                                update_data = current_series.copy()
                                
                                # Add the configured tag if it exists and isn't already added
                                if needs_tag_update:
                                    if 'tags' not in update_data:
                                        update_data['tags'] = []
                                    update_data['tags'].append(tag_id)
                                    print(f"{GREEN}Adding tag '{self.sonarr_config.get('sonarr_tag')}' to {show['title']}{RESET}")
                                
                                # Update monitoring only if monitoring option is not 'none'
                                if monitor_option != 'none':
                                    print(f"{GREEN}Updating monitoring status...{RESET}")
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
                                
                                if monitor_option != 'none':
                                    monitoring_message = 'all seasons' if monitor_option == 'all' else 'first season'
                                    print(f"{GREEN}Updated show and {monitoring_message} monitoring for: {show['title']}{RESET}")
                                
                                # If search_missing is enabled and monitoring is updated, trigger a search
                                if search_missing and monitor_option != 'none':
                                    search_cmd = {
                                        'name': 'MissingEpisodeSearch',
                                        'seriesId': existing_show['id']
                                    }
                                    sr = requests.post(f"{sonarr_url}/command", headers=headers, json=search_cmd)
                                    sr.raise_for_status()
                                    print(f"{GREEN}Triggered search for: {show['title']}{RESET}")
                            else:
                                print(f"{YELLOW}Already in Sonarr: {show['title']}{RESET}")
                            
                            # Skip to next show after handling the existing one
                            continue
                                
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
                        'seasonFolder': season_folder,
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

    if 'similarity_score' in show:
        score_percentage = round(show['similarity_score'] * 100, 1)
        output += f" - Similarity: {YELLOW}{score_percentage}%{RESET}"
		
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
    combine_watch_history = general.get('combine_watch_history', True)

    # Get all users that need to be processed
    all_users = []
    tautulli_config = base_config.get('tautulli', {})
    tautulli_users = tautulli_config.get('users')
    
    # Check if Tautulli is configured and users are not 'none'
    if tautulli_users and str(tautulli_users).lower() != 'none':
        # Process Tautulli users
        if isinstance(tautulli_users, str):
            all_users = [u.strip() for u in tautulli_users.split(',') if u.strip()]
        elif isinstance(tautulli_users, list):
            all_users = tautulli_users
    else:
        # Fall back to managed users if Tautulli is not configured or users is 'none'
        managed_users = base_config['plex'].get('managed_users', '')
        all_users = [u.strip() for u in managed_users.split(',') if u.strip()]

    if combine_watch_history or not all_users:
        # Original behavior - single run
        process_recommendations(base_config, config_path, keep_logs)
    else:
        # Individual runs for each user
        for user in all_users:
            print(f"\n{GREEN}Processing recommendations for user: {user}{RESET}")
            print("-" * 50)
            
            # Create modified config for this user
            user_config = copy.deepcopy(base_config)
            
            # Resolve Admin to actual username if needed
            resolved_user = user
            try:
                account = MyPlexAccount(token=base_config['plex']['token'])
                admin_username = account.username
                if user.lower() in ['admin', 'administrator']:
                    resolved_user = admin_username
                    print(f"{YELLOW}Resolved Admin to: {admin_username}{RESET}")
            except Exception as e:
                print(f"{YELLOW}Could not resolve admin username: {e}{RESET}")
            
            if 'managed_users' in user_config['plex']:
                user_config['plex']['managed_users'] = resolved_user
            elif 'users' in user_config.get('tautulli', {}):
                user_config['tautulli']['users'] = [resolved_user]
            
            # Process recommendations for this user
            process_recommendations(user_config, config_path, keep_logs, single_user=resolved_user)
            print(f"\n{GREEN}Completed processing for user: {resolved_user}{RESET}")
            print("-" * 50)

    runtime = datetime.now() - start_time
    hours = runtime.seconds // 3600
    minutes = (runtime.seconds % 3600) // 60
    seconds = runtime.seconds % 60
    print(f"\n{GREEN}All processing completed!{RESET}")
    print(f"Total runtime: {hours:02d}:{minutes:02d}:{seconds:02d}")

def process_recommendations(config, config_path, keep_logs, single_user=None):
    original_stdout = sys.stdout
    log_dir = os.path.join(os.path.dirname(__file__), 'Logs')
    
    if keep_logs > 0:
        try:
            os.makedirs(log_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            user_suffix = f"_{single_user}" if single_user else ""
            log_file_path = os.path.join(log_dir, f"recommendations{user_suffix}_{timestamp}.log")
            lf = open(log_file_path, "w", encoding="utf-8")
            sys.stdout = TeeLogger(lf)
            cleanup_old_logs(log_dir, keep_logs)
        except Exception as e:
            print(f"{RED}Could not set up logging: {e}{RESET}")

    try:
        # Create recommender with single user context
        recommender = PlexTVRecommender(config_path, single_user)
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
        if keep_logs > 0 and sys.stdout is not original_stdout:
            try:
                sys.stdout.logfile.close()
                sys.stdout = original_stdout
            except Exception as e:
                print(f"{YELLOW}Error closing log file: {e}{RESET}")
	
if __name__ == "__main__":
    main()