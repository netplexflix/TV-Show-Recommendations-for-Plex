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

class PlexTVRecommender:
    def __init__(self, config_path: str):
        print("Initializing recommendation system...")
        self.config = self._load_config(config_path)
        
        print("Connecting to Plex server...")
        self.plex = self._init_plex()
        print(f"Connected to Plex successfully!\n")
        
        general_config = self.config.get('general', {})
        self.confirm_operations = general_config.get('confirm_operations', False)
        self.limit_plex_results = general_config.get('limit_plex_results', 10)
        self.limit_trakt_results = general_config.get('limit_trakt_results', 10)
        self.show_summary = general_config.get('show_summary', False)
        self.plex_only = general_config.get('plex_only', False)
        self.show_cast = general_config.get('show_cast', False)
        self.show_creator = general_config.get('show_creator', False)
        self.show_language = general_config.get('show_language', False)
        self.show_imdb_link = general_config.get('show_imdb_link', False)
        
        exclude_genre_str = general_config.get('exclude_genre', '')
        self.exclude_genres = [g.strip().lower() for g in exclude_genre_str.split(',') if g.strip()] if exclude_genre_str else []

        weights_config = self.config.get('weights', {})
        self.weights = {
            'genre_weight': float(weights_config.get('genre_weight', 0.25)),
            'creator_weight': float(weights_config.get('creator_weight', 0.20)),
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

        tmdb_config = self.config.get('TMDB', {})
        self.use_tmdb_keywords = tmdb_config.get('use_TMDB_keywords', False)
        self.tmdb_api_key = tmdb_config.get('api_key', None)

        self.sonarr_config = self.config.get('sonarr', {})

        self.cache_dir = os.path.join(os.path.dirname(__file__), "cache")
        os.makedirs(self.cache_dir, exist_ok=True)
        
        self.watched_cache_path = os.path.join(self.cache_dir, "watched_data_cache.json")
        self.unwatched_cache_path = os.path.join(self.cache_dir, "unwatched_data_cache.json")
        self.trakt_sync_cache_path = os.path.join(self.cache_dir, "trakt_sync_cache.json")
        self.synced_trakt_episodes, self.last_watched_count = self._load_trakt_sync_cache()
        
        self.cached_watched_count, self.watched_data_counters, self.plex_tmdb_cache, self.tmdb_keywords_cache = self._load_watched_cache()
        self.cached_library_show_count, self.cached_unwatched_count, self.cached_unwatched_shows = self._load_unwatched_cache()
        self.synced_trakt_episodes, self.last_watched_count = self._load_trakt_sync_cache()
        
        if self.plex_tmdb_cache is None:
            self.plex_tmdb_cache = {}
        if self.tmdb_keywords_cache is None:
            self.tmdb_keywords_cache = {}
        if not hasattr(self, 'synced_trakt_history'):
            self.synced_trakt_history = {}

        self.library_title = self.config['plex'].get('TV_library_title', 'TV Shows')

        current_watched_count = self._get_watched_count()
        if current_watched_count != self.cached_watched_count:
            print("Watched count changed or no cache found; gathering watched data now. This may take a while...\n")
            self.watched_data = self._get_watched_shows_data()
            self.watched_data_counters = self.watched_data
            self.cached_watched_count = current_watched_count
            self._save_watched_cache()
        else:
            print("Watched count unchanged. Using cached data for faster performance.\n")
            self.watched_data = self.watched_data_counters

        print("Fetching library metadata (for existing show checks)...")
        self.library_shows = self._get_library_shows_set()

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
    def _load_watched_cache(self):
        if not os.path.exists(self.watched_cache_path):
            self.synced_trakt_history = {}
            return 0, {}, {}, {}
    
        try:
            with open(self.watched_cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
    
            # Ensure we're only counting unique episode IDs
            unique_watched_episodes = set(data.get('watched_episode_ids', []))
    
            return (
                len(unique_watched_episodes),  # Only count unique episodes
                data.get('watched_data_counters', {}),
                data.get('plex_tmdb_cache', {}),
                data.get('tmdb_keywords_cache', {})
            )
        except Exception as e:
            print(f"{YELLOW}Error loading watched cache: {e}{RESET}")
            self.synced_trakt_history = {}
            return 0, {}, {}, {}
    
    def _load_unwatched_cache(self):
        if not os.path.exists(self.unwatched_cache_path):
            return 0, 0, []
    
        try:
            with open(self.unwatched_cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
    
            unique_unwatched_episode_ids = set(data.get('unwatched_episode_ids', []))
    
            return (
                data.get('library_show_count', 0),
                len(unique_unwatched_episode_ids),  # Ensure unique count
                data.get('unwatched_show_details', [])
            )
        except Exception as e:
            print(f"{YELLOW}Error loading unwatched cache: {e}{RESET}")
            return 0, 0, []

    def _load_trakt_sync_cache(self):
        if not os.path.exists(self.trakt_sync_cache_path):
            return set(), 0  # Default to empty set and count 0
    
        try:
            with open(self.trakt_sync_cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            synced_episodes = set(data.get('synced_episodes', []))
            last_watched_count = data.get('watched_count', 0)  # Ensure this tracks unique episodes
            return synced_episodes, last_watched_count
        except Exception as e:
            print(f"{YELLOW}Error loading Trakt sync cache: {e}{RESET}")
            return set(), 0
			
    def _save_watched_cache(self):
        # Collect unique episode IDs
        watched_episode_ids = set()
        for show in self.watched_data_counters.get('episodes', {}).values():
            for ep in show:
                watched_episode_ids.add(str(ep['ratingKey']))
    
        data = {
            'watched_count': len(watched_episode_ids),  # Ensure we're storing only unique episode count
            'watched_episode_ids': list(watched_episode_ids),  # Store episode IDs for checking later
            'watched_data_counters': self.watched_data_counters,
            'plex_tmdb_cache': self.plex_tmdb_cache,
            'tmdb_keywords_cache': self.tmdb_keywords_cache,
            'synced_trakt_history': self.synced_trakt_history
        }
        try:
            with open(self.watched_cache_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            print(f"{YELLOW}Error saving watched cache: {e}{RESET}")
    
    def _save_unwatched_cache(self):
        # Collect unique unwatched episode IDs
        unwatched_episode_ids = set()
        for show in self.cached_unwatched_shows:
            for ep in show.get('episodes', []):
                unwatched_episode_ids.add(str(ep['ratingKey']))
    
        data = {
            'library_show_count': self.cached_library_show_count,
            'unwatched_count': len(unwatched_episode_ids),  # Store only unique episode count
            'unwatched_episode_ids': list(unwatched_episode_ids),  # Store for tracking
            'unwatched_show_details': self.cached_unwatched_shows
        }
        try:
            with open(self.unwatched_cache_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            print(f"{YELLOW}Error saving unwatched cache: {e}{RESET}")

    def _save_trakt_sync_cache(self):
        data = {
            'synced_episodes': list(self.synced_trakt_episodes),
            'watched_count': self.current_watched_count  # Correctly tracks only unique episode IDs
        }
        try:
            with open(self.trakt_sync_cache_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            print(f"{YELLOW}Error saving Trakt sync cache: {e}{RESET}")
			
    def _save_cache(self):
        self._save_watched_cache()
        self._save_unwatched_cache()
        self._save_trakt_sync_cache()

    def _get_watched_count(self) -> int:
        try:
            shows_section = self.plex.library.section(self.library_title)
            watched_episodes = shows_section.searchEpisodes(unwatched=False)
    
            unique_episode_ids = {str(ep.ratingKey) for ep in watched_episodes}  # Track only unique episodes
            return len(unique_episode_ids)  # Only count unique episodes
        except Exception as e:
            print(f"{RED}Error getting watched episode count: {e}{RESET}")
            return 0

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
    def _get_library_shows_set(self) -> Set[Tuple[str, Optional[int]]]:
        try:
            shows = self.plex.library.section(self.library_title)
            return {(show.title.lower(), getattr(show, 'year', None)) for show in shows.all()}
        except Exception as e:
            print(f"{RED}Error getting library shows: {e}{RESET}")
            return set()

    def _is_show_in_library(self, title: str, year: Optional[int]) -> bool:
        return (title.lower(), year) in self.library_shows

    # ------------------------------------------------------------------------
    # TMDB HELPER METHODS
    # ------------------------------------------------------------------------
    def _get_plex_show_tmdb_id(self, plex_show) -> Optional[int]:
        if not self.use_tmdb_keywords or not self.tmdb_api_key:
            return None

        if plex_show.ratingKey in self.plex_tmdb_cache:
            return self.plex_tmdb_cache[plex_show.ratingKey]

        tmdb_id = None
        if hasattr(plex_show, 'guid'):
            guid = plex_show.guid
            if 'themoviedb://' in guid:
                try:
                    tmdb_id = int(guid.split('themoviedb://')[1])
                except:
                    pass

        if not tmdb_id:
            title = plex_show.title
            year = getattr(plex_show, 'year', None)
            if not title:
                self.plex_tmdb_cache[plex_show.ratingKey] = None
                return None
            try:
                base_url = "https://api.themoviedb.org/3/search/show"
                params = {'api_key': self.tmdb_api_key, 'query': title}
                if year:
                    params['first_air_date_year'] = year

                resp = requests.get(base_url, params=params)
                if resp.status_code == 200:
                    data = resp.json()
                    results = data.get('results', [])
                    if results:
                        if year:
                            for r in results:
                                if r.get('first_air_date', '').startswith(str(year)):
                                    tmdb_id = r['id']
                                    break
                        if not tmdb_id:
                            tmdb_id = results[0]['id']
            except Exception as e:
                print(f"{YELLOW}Could not fetch TMDB ID for '{title}': {e}{RESET}")

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

    def _get_watched_shows_data(self) -> Dict:
        genre_counter = Counter()
        creator_counter = Counter()
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
                        genre_counter[g.tag.lower()] += rating_weight

                if hasattr(show, 'creators') and show.creators:
                    for c in show.creators:
                        creator_counter[c.tag] += rating_weight

                if hasattr(show, 'roles') and show.roles:
                    for a in show.roles:
                        actor_counter[a.tag] += rating_weight

                if self.show_language:
                    try:
                        media = show.media[0]
                        part = media.parts[0]
                        audio_streams = part.audioStreams()
                        if audio_streams:
                            primary_audio = audio_streams[0]
                            lang_code = (
                                getattr(primary_audio, 'languageTag', None) or
                                getattr(primary_audio, 'languageCode', None) or
                                getattr(primary_audio, 'language', None)
                            )
                            if lang_code:
                                language = get_full_language_name(lang_code)
                                language_counter[language.lower()] += rating_weight
                    except:
                        pass

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
            'creators': dict(creator_counter),
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
        if not self.sync_watch_history:
            return
    
        print(f"{YELLOW}Syncing Plex watch history to Trakt...{RESET}")
    
        shows_section = self.plex.library.section(self.library_title)
        try:
            watched_episodes = shows_section.searchEpisodes(unwatched=False)
        except Exception as e:
            print(f"{RED}Error fetching watched episodes: {e}{RESET}")
            return
    
        # Step 1: Track unique episodes (latest watch time per episode)
        watched_episodes_dict = {}
        for ep in watched_episodes:
            ep_id = str(ep.ratingKey)
            last_viewed = getattr(ep, 'lastViewedAt', None)
            if last_viewed:
                # Store only the last viewed timestamp for each episode
                if ep_id not in watched_episodes_dict or last_viewed > watched_episodes_dict[ep_id]['lastViewedAt']:
                    watched_episodes_dict[ep_id] = {
                        'episode': ep,
                        'lastViewedAt': last_viewed
                    }
    
        self.current_watched_count = len(watched_episodes_dict)
    
        # Step 2: If watched count is unchanged, skip syncing
        if self.current_watched_count == self.last_watched_count:
            print(f"{GREEN}Watched episode count unchanged ({self.current_watched_count}). Skipping Trakt sync.{RESET}")
            return
    
        # Step 3: Identify new episodes to sync
        new_episodes = {
            ep_id: ep_data for ep_id, ep_data in watched_episodes_dict.items()
            if ep_id not in self.synced_trakt_episodes  # Only new unique episodes
        }
    
        if not new_episodes:
            print(f"{GREEN}No new episodes found to sync to Trakt. Skipping sync.{RESET}")
            return
    
        to_sync = []
        show_season_episodes = defaultdict(lambda: defaultdict(list))
        
        # Step 4: Fetch Trakt show IDs and prepare the payload
        total_new = len(new_episodes)
        for i, (ep_id, ep_data) in enumerate(new_episodes.items(), start=1):
            episode = ep_data['episode']
            last_viewed = ep_data['lastViewedAt']
    
            try:
                show = episode.show()
                title = show.title
                year = getattr(show, 'year', None)
                season_num = episode.seasonNumber
                episode_num = episode.episodeNumber
                watched_at = last_viewed.strftime("%Y-%m-%dT%H:%M:%SZ")
    
                # Fetch Trakt show ID
                trakt_search_url = f"https://api.trakt.tv/search/show?query={quote(title)}"
                if year:
                    trakt_search_url += f"&year={year}"
    
                resp = requests.get(trakt_search_url, headers=self.trakt_headers)
                if resp.status_code != 200:
                    continue
    
                results = resp.json()
                if not results:
                    continue
    
                # Find best match
                trakt_show = next(
                    (r for r in results
                     if r['show']['title'].lower() == title.lower()
                     and str(r['show'].get('year', '')) == str(year)),
                    results[0]
                )
                trakt_id = trakt_show['show']['ids'].get('trakt')
                if not trakt_id:
                    continue
    
                # Add to payload
                show_season_episodes[trakt_id][season_num].append({
                    "number": episode_num,
                    "watched_at": watched_at
                })
                to_sync.append(ep_id)
    
                # Display progress
                self._show_progress("Processing new episodes", i, total_new)
    
            except Exception as e:
                print(f"{RED}Error processing {title} S{season_num}E{episode_num}: {e}{RESET}")
    
        print(f"\n{GREEN}Found {len(to_sync)} new episodes to sync{RESET}")
    
        if not to_sync:
            print(f"{GREEN}No new episodes found to sync to Trakt. Skipping sync.{RESET}")
            return
    
        # Step 5: Sync only new episodes
        shows_payload = []
        for trakt_id, seasons in show_season_episodes.items():
            seasons_list = []
            for season_num, episodes in seasons.items():
                seasons_list.append({
                    "number": season_num,
                    "episodes": [{"number": ep["number"], "watched_at": ep["watched_at"]} for ep in episodes]
                })
            shows_payload.append({
                "ids": {"trakt": trakt_id},
                "seasons": seasons_list
            })
    
        # Send to Trakt in chunks
        CHUNK_SIZE = 50
        for i in range(0, len(shows_payload), CHUNK_SIZE):
            chunk = shows_payload[i:i+CHUNK_SIZE]
            try:
                response = requests.post(
                    "https://api.trakt.tv/sync/history",
                    headers=self.trakt_headers,
                    json={"shows": chunk}
                )
                response.raise_for_status()
                self.synced_trakt_episodes.update(to_sync[i:i+CHUNK_SIZE])
                self._save_trakt_sync_cache()
                print(f"{GREEN}Synced chunk {i//CHUNK_SIZE + 1} successfully{RESET}")
            except Exception as e:
                print(f"{RED}Failed to sync chunk {i//CHUNK_SIZE + 1}: {e}{RESET}")
                break
    
        print(f"{GREEN}Synced {len(to_sync)} new episodes to Trakt{RESET}")
    
        # Step 6: Update watched count to reflect unique episodes
        self.last_watched_count = self.current_watched_count

    def calculate_show_score(self, show) -> float:
        user_genres = Counter(self.watched_data['genres'])
        user_creators = Counter(self.watched_data['creators'])
        user_acts = Counter(self.watched_data['actors'])
        user_kws  = Counter(self.watched_data['tmdb_keywords'])
        user_langs = Counter(self.watched_data.get('languages', {}))  # Use .get to avoid KeyError

        weights = self.weights

        max_genre_count = max(user_genres.values(), default=1)
        max_creator_count = max(user_creators.values(), default=1)
        max_actor_count = max(user_acts.values(), default=1)
        max_keyword_count = max(user_kws.values(), default=1)
        max_language_count = max(user_langs.values(), default=1)

        score = 0.0
        if hasattr(show, 'genres') and show.genres:
            show_genres = {g.tag.lower() for g in show.genres}
            gscore = 0.0
            for sg in show_genres:
                gcount = user_genres.get(sg, 0)
                gscore += (gcount / max_genre_count) if max_genre_count else 0
            if len(show_genres) > 0:
                gscore /= len(show_genres)
            score += gscore * weights.get('genre_weight', 0.25)

        if hasattr(show, 'creators') and show.creators:
            cscore = 0.0
            matched_creators = 0
            for c in show.creators:
                if c.tag in user_creators:
                    matched_creators += 1
                    cscore += (user_creators[c.tag] / max_creator_count)
            if matched_creators > 0:
                cscore /= matched_creators
            score += cscore * weights.get('creator_weight', 0.20)

        if hasattr(show, 'roles') and show.roles:
            ascore = 0.0
            matched_actors = 0
            for a in show.roles:
                if a.tag in user_acts:
                    matched_actors += 1
                    ascore += (user_acts[a.tag] / max_actor_count)
            if matched_actors > 3:
                ascore *= (3 / matched_actors)
            if matched_actors > 0:
                ascore /= matched_actors
            score += ascore * weights.get('actor_weight', 0.20)

        if hasattr(show, 'media') and self.show_language:
            try:
                media = show.media[0]
                part = media.parts[0]
                audio_streams = part.audioStreams()
                if audio_streams:
                    primary_audio = audio_streams[0]
                    lang_code = (
                        getattr(primary_audio, 'languageTag', None) or
                        getattr(primary_audio, 'languageCode', None) or
                        getattr(primary_audio, 'language', None)
                    )
                    if lang_code:
                        language = get_full_language_name(lang_code).lower()
                        lcount = user_langs.get(language, 0)
                        lscore = (lcount / max_language_count) if max_language_count else 0
                        score += lscore * weights.get('language_weight', 0.10)
            except:
                pass

        if self.use_tmdb_keywords and self.tmdb_api_key:
            tmdb_id = self._get_plex_show_tmdb_id(show)
            if tmdb_id:
                keywords = self._get_tmdb_keywords_for_id(tmdb_id)
                kwscore = 0.0
                matched_kw = 0
                for kw in keywords:
                    count = user_kws.get(kw, 0)
                    if count > 0:
                        matched_kw += 1
                        kwscore += (count / max_keyword_count)
                if matched_kw > 0:
                    kwscore /= matched_kw
                score += kwscore * weights.get('keyword_weight', 0.25)

        return score

    def get_show_details(self, show) -> Dict:
        try:
            show.reload()
        except Exception as e:
            print(f"{YELLOW}Warning: Could not reload show '{show.title}': {e}{RESET}")
        ratings = {}
        if hasattr(show, 'rating'):
            ratings['imdb_rating'] = round(float(show.rating), 1) if show.rating else 0
        if hasattr(show, 'audienceRating'):
            ratings['audience_rating'] = round(float(show.audienceRating), 1) if show.audienceRating else 0
        if hasattr(show, 'ratingCount'):
            ratings['votes'] = show.ratingCount

        sim_score = self.calculate_show_score(show)

        cast_list = []
        creator_name = "N/A"
        language_str = "N/A"
        imdb_id = None

        if self.show_cast or self.show_creator:
            if hasattr(show, 'roles'):
                cast_list = [r.tag for r in show.roles[:3]]

            if hasattr(show, 'creators') and show.creators:
                creator_name = show.creators[0].tag

        if self.show_language:
            try:
                media = show.media[0]
                part = media.parts[0]
                audio_streams = part.audioStreams()
                if audio_streams:
                    primary_audio = audio_streams[0]
                    lang_code = (
                        getattr(primary_audio, 'languageTag', None) or
                        getattr(primary_audio, 'languageCode', None) or
                        getattr(primary_audio, 'language', None)
                    )
                    if lang_code:
                        language = get_full_language_name(lang_code).lower()
                        language_str = language.capitalize()
            except:
                pass

        if self.show_imdb_link:
            try:
                imdb_id = self._get_plex_show_imdb_id(show)
            except Exception as e:
                print(f"{YELLOW}Error fetching IMDb ID for '{show.title}': {e}{RESET}")

        return {
            'title': show.title,
            'year': getattr(show, 'year', None),
            'genres': [g.tag.lower() for g in show.genres] if hasattr(show, 'genres') else [],
            'summary': getattr(show, 'summary', ''),
            'ratings': ratings,
            'similarity_score': sim_score,
            'cast': cast_list,
            'creator': creator_name,
            'language': language_str,
            'imdb_id': imdb_id
        }

    def get_unwatched_library_shows(self) -> List[Dict]:
        print(f"\n{YELLOW}Fetching unwatched shows from Plex library...{RESET}")
        shows_section = self.plex.library.section(self.library_title)
        
        current_all = shows_section.all()
        current_all_count = len(current_all)
        current_unwatched = shows_section.search(unwatched=True)
        current_unwatched_count = len(current_unwatched)
    
        if (current_all_count == self.cached_library_show_count and
            current_unwatched_count == self.cached_unwatched_count):
            print(f"Unwatched count unchanged. Using cached data for faster performance.")
            return self.cached_unwatched_shows
    
        unwatched_details = []
        for i, show in enumerate(current_unwatched, start=1):
            self._show_progress("Scanning unwatched", i, current_unwatched_count)
            info = self.get_show_details(show)
            
            unwatched_details.append(info)
        print()
    
        print(f"Found {len(unwatched_details)} unwatched shows matching your criteria.\n")
    
        self.cached_library_show_count = current_all_count
        self.cached_unwatched_count = current_unwatched_count
        self.cached_unwatched_shows = unwatched_details
        self._save_unwatched_cache()
        return unwatched_details

    def get_trakt_recommendations(self) -> List[Dict]:
        print(f"{YELLOW}Fetching recommendations from Trakt...{RESET}")
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
                            'creator': "N/A",
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
    
                            if self.show_cast or self.show_creator:
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
    
                                    if self.show_creator and 'crew' in c_data:
                                        creators = [p for p in c_data['crew'] if p.get('job') == 'Creator']
                                        if creators:
                                            sd['creator'] = creators[0]['name']
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

            print(f"Excluded {len(excluded_recs)} shows based on excluded genres.")

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
                       show_creator: bool = False,
                       show_language: bool = False,
                       show_imdb_link: bool = False) -> str:
    bullet = f"{index}. " if index is not None else "- "
    output = f"{bullet}{CYAN}{show['title']}{RESET} ({show.get('year', 'N/A')})"
    
    if show.get('genres'):
        output += f"\n  {YELLOW}Genres:{RESET} {', '.join(show['genres'])}"

    if 'ratings' in show and 'imdb_rating' in show['ratings'] and show['ratings']['imdb_rating'] > 0:
        votes_str = ""
        if 'votes' in show['ratings']:
            votes_str = f" ({show['ratings']['votes']} votes)"
        output += f"\n  {YELLOW}IMDb Rating:{RESET} {show['ratings']['imdb_rating']}/10{votes_str}"

    if show_summary and show.get('summary'):
        output += f"\n  {YELLOW}Summary:{RESET} {show['summary']}"

    if show_cast and 'cast' in show and show['cast']:
        cast_str = ', '.join(show['cast'])
        output += f"\n  {YELLOW}Cast:{RESET} {cast_str}"

    if show_creator and 'creator' in show and show['creator'] != "N/A":
        output += f"\n  {YELLOW}Creator:{RESET} {show['creator']}"

    if show_language and 'language' in show and show['language'] != "N/A":
        output += f"\n  {YELLOW}Language:{RESET} {show['language']}"

    if show_imdb_link and 'imdb_id' in show and show['imdb_id']:
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
                    show_creator=recommender.show_creator,
                    show_language=recommender.show_language,
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
                        show_creator=recommender.show_creator,
                        show_language=recommender.show_language,
                        show_imdb_link=recommender.show_imdb_link
                    ))
                    print()
                recommender.add_to_sonarr(trakt_recs)
            else:
                print(f"{YELLOW}No Trakt recommendations found matching your criteria.{RESET}")

        recommender._save_cache()

    except Exception as e:
        print(f"\n{RED}An error occurred: {e}{RESET}")
        import traceback
        print(traceback.format_exc())

    if keep_logs > 0 and sys.stdout is not original_stdout:
        try:
            sys.stdout.logfile.close()
            sys.stdout = original_stdout
        except Exception as e:
            print(f"{YELLOW}Error closing log file: {e}{RESET}")

    print(f"\n{GREEN}Process completed!{RESET}")

if __name__ == "__main__":
    main()
