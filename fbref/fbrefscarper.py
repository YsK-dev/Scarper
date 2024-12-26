from bs4 import BeautifulSoup as soup
import requests
import pandas as pd
import time
import re
from functools import reduce
import sys
from urllib.error import HTTPError



def get_data_info():
    # all possible leagues and seasons Firstly started from 2017 because from 2017 there is more stats about both matchesy and player
    leagues = ['Premier League', 'La Liga', 'Serie A', 'Ligue 1', 'Bundesliga', 'Super Lig'] 
    seasons = ['2017-2018', '2018-2019', '2019-2020', '2020-2021', '2021-2022','2022-2023','2023-2024','2024-2025']
    
    while True:
        # select league [Premier League / La Liga / Serie A / Ligue 1 / Bundesliga]
        league = input('Select League (PrePmier League / La Liga / Serie A / Ligue 1 / Bundesliga / Super Lig): ')
        
        # check if input valid
        if league not in leagues:
            print('League not valid, try again')
            continue
            
        # assign url names and id's
        if league == 'Premier League':
            league = 'Premier-League'
            league_id = '9'

        if league == 'La Liga':
            league = 'La-Liga'
            league_id = '12'

        if league == 'Serie A':
            league = 'Serie-A'
            league_id = '11'

        if league == 'Ligue 1':
            league = 'Ligue-1'
            league_id = '13'

        if league == 'Bundesliga':
            league = 'Bundesliga'
            league_id = '20'

        if league == 'Super Lig':
            league = 'Super-Lig'
            league_id = '26'

        break
            
    while True: 
        # select season after 2017 as XG only available from 2017 thats why I started from 2017 I could start from 2000 but then I wouldnt have that much stats and at the end of the day I did scrap from 2000 too but as I said it is not rich as this ones
        season = input('Select Season (2017-2018, 2018-2019, 2019-2020, 2020-2021, 2021-2022, 2022-2023, 2023-2024, 2024-2025)--> ')
        
        # check if input valid
        if season not in seasons:
            print('Season not valid, try again')
            continue
        break
#https://fbref.com/en/comps/9/2022-2023/schedule/2022-2023-Premier-League-Scores-and-Fixtures
    url = f'https://fbref.com/en/comps/{league_id}/{season}/schedule/{season}-{league}-Scores-and-Fixtures'
    return url, league, season


def get_fixture_data(url, league, season):
    print('Getting fixture data...')
    # create empty data frame and access all tables in url
    fixturedata = pd.DataFrame([])
    tables = pd.read_html(url)
    
    # get fixtures
    fixtures = tables[0][['Wk', 'Day', 'Date', 'Time', 'Home', 'Away', 'Score','xG', 'xG.1' ]].dropna()# for super lig there was no xg values 'xG','xG1'
    fixtures['season'] = url.split('/')[6]
    fixturedata = pd.concat([fixturedata,fixtures])
    
    # assign id for each game
    fixturedata["game_id"] = fixturedata.index
    
    # export to csv file
    fixturedata.reset_index(drop=True).to_csv(f'{league.lower()}_{season.lower()}_fixture_data.csv', 
        header=True, index=False, mode='w')
    print('Fixture data collected finally...')


def get_match_links(url, league):   
    print('Getting player data...')
    # access and download content from url containing all fixture links    
    match_links = []
    html = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    links = soup(html.content, "html.parser").find_all('a')
    
    # filter list to return only needed links
    key_words_good = ['/en/matches/', f'{league}']
    for l in links:
        href = l.get('href', '')
        if all(x in href for x in key_words_good):
            if 'https://fbref.com' + href not in match_links:                 
                match_links.append('https://fbref.com' + href)
    return match_links

def player_data(match_links, league, season):
    # Loop through all fixtures
    player_data = pd.DataFrame([])
    for count, link in enumerate(match_links):
        try:
            tables = pd.read_html(link)
            
            # Skip matches if not enough data is available
            if len(tables) < 17:  # Adjust based on expected number of tables for other leagues
                print(f"{link}: insufficient data for Super Lig, skipping")
                continue
            
            for table in tables:
                try:
                    table.columns = table.columns.droplevel()
                except Exception:
                    continue

            # Adjust the indices of tables for Super Lig
            if league == "Super-Lig":
                t1_idx = 3
                t2_idx = 9
            else:
                t1_idx = 3
                t2_idx = 10
            
            # Get player data
            def get_team_1_player_data():
                # Handle table availability for Super Lig
                data_frames = [tables[t1_idx]] if len(tables) > t1_idx else []
                if not data_frames:
                    return pd.DataFrame()
                df = pd.concat(data_frames).iloc[:1]
                return df.assign(home=1, game_id=count)

            def get_team_2_player_data():
                data_frames = [tables[t2_idx]] if len(tables) > t2_idx else []
                if not data_frames:
                    return pd.DataFrame()
                df = pd.concat(data_frames).iloc[:1]
                return df.assign(home=0, game_id=count)

            # Combine both teams' data
            t1 = get_team_1_player_data()
            t2 = get_team_2_player_data()
            combined_data = pd.concat([t1, t2]).reset_index()
            
            if not combined_data.empty:
                player_data = pd.concat([player_data, combined_data])
                print(f"{count+1}/{len(match_links)} matches collected")
                player_data.to_csv(f'{league.lower()}_{season.lower()}_player_data.csv', 
                                   header=True, index=False, mode='w')
            else:
                print(f"{link}: no data collected")

        except Exception as e:
            print(f"{link}: error - {e}")
        # Sleep to avoid IP being blocked
        time.sleep(7)



# main function
def main(): 
    url, league, season = get_data_info()
    get_fixture_data(url, league, season)
    match_links = get_match_links(url, league)
    player_data(match_links, league, season)

    # checks if user wants to collect more data
    print('Finally Data collected ehehe🤓')
    while True:
        answer = input('Do you want to collect more data? (yes/no): ')
        if answer == 'yes':
            main()
        if answer == 'no':
            sys.exit()
        else:
            print('Answer not valid')
            continue


if __name__ == '__main__':
    try:
        main()
    except HTTPError:
        print('The website refused access, try again later')
        time.sleep(5)

