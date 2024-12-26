from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
import random
import sys
from urllib.error import HTTPError
from functools import reduce
from typing import List, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class FBRefScraper:
    LEAGUES = {
        'Premier League': ('Premier-League', '9'),
        'La Liga': ('La-Liga', '12'),
        'Serie A': ('Serie-A', '11'),
        'Ligue 1': ('Ligue-1', '13'),
        'Bundesliga': ('Bundesliga', '20'),
        'Super Lig': ('Super-Lig', '26')
    }
    START_SEASON = 2017
    END_SEASON = 2024

    def __init__(self):
        self.session = self._create_session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'en-US,en;q=0.9',
        }

    def _create_session(self) -> requests.Session:
        """Create a session with retry strategy"""
        session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _make_request(self, url: str) -> str:
        """Make a request with proper error handling and rate limiting"""
        time.sleep(random.uniform(3, 7))  # Random delay between requests
        try:
            response = self.session.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            if hasattr(e.response, 'status_code') and e.response.status_code == 429:
                wait_time = int(e.response.headers.get('Retry-After', 60))
                print(f'Rate limited. Waiting {wait_time} seconds...')
                time.sleep(wait_time)
                return self._make_request(url)  # Retry after waiting
            raise

    def get_data_info(self) -> List[Tuple[str, str, str]]:
        """Get league information and generate URLs"""
        while True:
            league = input(f'Select League ({" / ".join(self.LEAGUES.keys())}): ')
            if league in self.LEAGUES:
                league_name, league_id = self.LEAGUES[league]
                break
            print('League not valid, try again')

        urls = []
        for season in range(self.START_SEASON, self.END_SEASON + 1):
            next_season = season + 1
            url = f'https://fbref.com/en/comps/{league_id}/{season}-{next_season}/schedule/{season}-{next_season}-{league_name}-Scores-and-Fixtures'
            urls.append((url, league_name, f"{season}-{next_season}"))
        
        return urls

    def get_fixture_data(self, url: str, league: str, season: str) -> None:
        """Scrape and save fixture data"""
        print(f'Getting fixture data for {league} {season}...')
        try:
            html_content = self._make_request(url)
            tables = pd.read_html(html_content)
            fixtures = tables[0]
            
            # Get available columns (some seasons might not have xG)
            available_columns = ['Wk', 'Day', 'Date', 'Time', 'Home', 'Away', 'Score']
            if 'xG' in fixtures.columns and 'xG.1' in fixtures.columns:
                available_columns.extend(['xG', 'xG.1'])
            
            fixtures = fixtures[available_columns].dropna(subset=['Home', 'Away'])
            fixtures['season'] = season
            fixtures["game_id"] = fixtures.index
            
            filename = f'{league.lower()}_{season.lower()}_fixture_data.csv'
            fixtures.reset_index(drop=True).to_csv(filename, index=False)
            print(f'Fixture data collected for {season}')
            return fixtures
        except Exception as e:
            print(f'Error collecting fixture data for {season}: {str(e)}')
            return None

    def get_match_links(self, url: str, league: str) -> List[str]:
        """Get match links from the league page"""
        print('Getting match links...')
        try:
            html_content = self._make_request(url)
            soup = BeautifulSoup(html_content, "html.parser")
            
            match_links = [
                'https://fbref.com' + link.get('href')
                for link in soup.find_all('a')
                if '/en/matches/' in link.get('href', '') and league in link.get('href', '')
            ]
            return list(set(match_links))
        except Exception as e:
            print(f'Error getting match links: {str(e)}')
            return []

    def get_player_data(self, match_links: List[str], league: str, season: str) -> None:
        """Scrape and save player data"""
        print(f'Getting player data for {league} {season}...')
        player_data = pd.DataFrame()
        
        for count, link in enumerate(match_links, 1):
            try:
                html_content = self._make_request(link)
                tables = pd.read_html(html_content)
                
                # Process home team data
                try:
                    home_data = pd.concat([
                        tables[3].assign(home=1, game_id=count),
                        tables[9].assign(home=1, game_id=count)
                    ], ignore_index=True)
                    
                    # Process away team data
                    away_data = pd.concat([
                        tables[10].assign(home=0, game_id=count),
                        tables[16].assign(home=0, game_id=count)
                    ], ignore_index=True)
                    
                    # Drop totals rows and combine data
                    home_data = home_data[home_data['Player'] != 'Team Total']
                    away_data = away_data[away_data['Player'] != 'Team Total']
                    
                    match_data = pd.concat([home_data, away_data], ignore_index=True)
                    player_data = pd.concat([player_data, match_data], ignore_index=True)
                    
                    print(f'Progress: {count}/{len(match_links)} matches collected')
                    
                    # Save after each successful match
                    filename = f'{league.lower()}_{season.lower()}_player_data.csv'
                    player_data.to_csv(filename, index=False)
                    
                except (KeyError, IndexError) as e:
                    print(f'Error processing tables for match {link}: {str(e)}')
                    continue
                    
            except Exception as e:
                print(f'Error processing match {link}: {str(e)}')
                continue

def main():
    scraper = FBRefScraper()
    
    while True:
        try:
            urls = scraper.get_data_info()
            
            for url, league, season in urls:
                print(f"\nProcessing {league} for season {season}")
                try:
                    if scraper.get_fixture_data(url, league, season):
                        match_links = scraper.get_match_links(url, league)
                        if match_links:
                            scraper.get_player_data(match_links, league, season)
                        else:
                            print(f"No match links found for {league} {season}")
                except Exception as e:
                    print(f"Error processing {league} {season}: {str(e)}")
                    continue
                
                time.sleep(random.uniform(5, 10))  # Delay between seasons
            
            print('\nAll data collection completed! 🎉')
            break
            
        except KeyboardInterrupt:
            print("\nScript interrupted by user. Exiting...")
            break
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")
            if input("Do you want to try again? (yes/no): ").lower() != 'yes':
                break

if __name__ == '__main__':
    main()