from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QLabel, QComboBox, QPushButton, QVBoxLayout, QWidget, QTextEdit)
import sys
from bs4 import BeautifulSoup as soup
import requests
import pandas as pd
import time
from functools import reduce

class FootballDataApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Football Data  for fbref")

        # Main layout
        self.layout = QVBoxLayout()

        # Dropdown for leagues
        self.league_label = QLabel("Select League -->")
        self.layout.addWidget(self.league_label)

        self.league_dropdown = QComboBox()
        self.league_dropdown.addItems([
            "Premier League", "La Liga", "Serie A", "Ligue 1", "Bundesliga", "Super Lig"
        ])
        self.layout.addWidget(self.league_dropdown)

        # Dropdown for seasons
        self.season_label = QLabel("Select Season:")
        self.layout.addWidget(self.season_label)

        self.season_dropdown = QComboBox()
        self.season_dropdown.addItems([
            "2017-2018", "2018-2019", "2019-2020", "2020-2021", "2021-2022",
            "2022-2023", "2023-2024", "2024-2025"
        ])
        self.layout.addWidget(self.season_dropdown)

        # Button to start scraping
        self.scrape_button = QPushButton("Start Scraping")
        self.scrape_button.clicked.connect(self.start_scraping)
        self.layout.addWidget(self.scrape_button)

        # Text box for logs to see errors or other logs
        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)
        self.layout.addWidget(self.log_box)

        # Central widget
        central_widget = QWidget()
        central_widget.setLayout(self.layout)
        self.setCentralWidget(central_widget)

    def log(self, message):
        self.log_box.append(message)
        self.log_box.verticalScrollBar().setValue(self.log_box.verticalScrollBar().maximum())

    def start_scraping(self):
        league = self.league_dropdown.currentText()
        season = self.season_dropdown.currentText()

        league_id = {
            "Premier League": "9",
            "La Liga": "12",
            "Serie A": "11",
            "Ligue 1": "13",
            "Bundesliga": "20",
            "Super Lig": "26"
        }[league]

        league_url_name = league.replace(" ", "-")
        url = f'https://fbref.com/en/comps/{league_id}/{season}/schedule/{season}-{league_url_name}-Scores-and-Fixtures'

        self.log(f"Scraping fixtures for {league} ({season})...")

        try:
            get_fixture_data(url, league, season)
            match_links = get_match_links(url, league)
            player_data(match_links, league, season)
            self.log("Data collection completed!")
        except Exception as e:
            self.log(f"Error: {e}")

# Functions for scraping

def get_fixture_data(url, league, season):
    tables = pd.read_html(url)
    fixtures = tables[0][['Wk', 'Day', 'Date', 'Time', 'Home', 'Away', 'xG', 'xG.1', 'Score']].dropna()
    fixtures['season'] = url.split('/')[6]
    fixtures["game_id"] = fixtures.index
    fixtures.reset_index(drop=True).to_csv(
        f'{league.lower()}_{season.lower()}_fixture_data.csv', header=True, index=False, mode='w')

def get_match_links(url, league):
    match_links = []
    html = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    links = soup(html.content, "html.parser").find_all('a')
    key_words_good = ['/en/matches/', f'{league.replace(" ", "-")}']
    for l in links:
        href = l.get('href', '')
        if all(x in href for x in key_words_good):
            full_url = 'https://fbref.com' + href
            if full_url not in match_links:
                match_links.append(full_url)
    return match_links

def player_data(match_links, league, season):
    player_data = pd.DataFrame([])
    for count, link in enumerate(match_links):
        try:
            tables = pd.read_html(link)
            for table in tables:
                try:
                    table.columns = table.columns.droplevel()
                except Exception:
                    continue

            def get_team_1_player_data():
                data_frames = [tables[3], tables[9]]
                df = reduce(lambda left, right: pd.merge(left, right, 
                            on=['Player', 'Nation', 'Age', 'Min'], how='outer'), data_frames).iloc[:-1]
                return df.assign(home=1, game_id=count)

            def get_team_2_player_data():
                data_frames = [tables[10], tables[16]]
                df = reduce(lambda left, right: pd.merge(left, right, 
                            on=['Player', 'Nation', 'Age', 'Min'], how='outer'), data_frames).iloc[:-1]
                return df.assign(home=0, game_id=count)

            t1 = get_team_1_player_data()
            t2 = get_team_2_player_data()
            player_data = pd.concat([player_data, pd.concat([t1, t2]).reset_index()])
            player_data.to_csv(f'{league.lower()}_{season.lower()}_player_data.csv', 
                               header=True, index=False, mode='w')
        except:
            continue
        time.sleep(3)

# Run the app
if __name__ == '__main__':
    app = QApplication(sys.argv)
    main_window = FootballDataApp()
    main_window.show()
    sys.exit(app.exec_())
