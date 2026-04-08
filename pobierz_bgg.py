#!/usr/bin/env python3
import requests
import sqlite3
import time
import logging
import xml.etree.ElementTree as ET
import os
import datetime
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# --- Konfiguracja ---
load_dotenv()
DB_FILE = "gamefound.db"
BGG_THING_URL = "https://boardgamegeek.com/xmlapi2/thing"
BGG_TOKEN = os.getenv('BGG_TOKEN')
MIN_REQUEST_INTERVAL = 2
BATCH_SIZE = 400          # ile gier pobrać w jednym uruchomieniu
LAST_ID_FILE = os.path.join(os.path.dirname(__file__), 'last_bgg_id.txt')

# --- Konfiguracja maila ---
EMAIL_HOST = os.getenv('EMAIL_HOST')
EMAIL_PORT = int(os.getenv('EMAIL_PORT'))
EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_PASS = os.getenv('EMAIL_PASS')
EMAIL_RECIPIENT = os.getenv('EMAIL_RECIPIENT')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Funkcje pomocnicze ---
def wait_for_rate_limit():
    global last_request_time
    elapsed = time.time() - last_request_time
    if elapsed < MIN_REQUEST_INTERVAL:
        time.sleep(MIN_REQUEST_INTERVAL - elapsed)
    last_request_time = time.time()

last_request_time = 0

def fetch_game(bgg_id):
    """Pobiera dane gry z BGG i zwraca krotkę (name, description, stats)."""
    try:
        wait_for_rate_limit()
        params = {'id': bgg_id, 'stats': 1, 'versions': 0, 'videos': 0, 'comments': 0}
        headers = {'Authorization': f'Bearer {BGG_TOKEN}'}
        response = requests.get(BGG_THING_URL, params=params, headers=headers, timeout=10)
	#
        # ⬅OBSŁUGA 4299 - zadużo zapytań #
        if response.status_code == 429:
            logging.warning(f"Limit zapytań BGG dla ID {bgg_id} – czekam 10 sekund")
            time.sleep(10)
            return None, None, None

        if response.status_code != 200:
            logging.warning(f"Błąd pobierania {bgg_id}: {response.status_code}")
            return None, None, None

        root = ET.fromstring(response.text)
        item = root.find('.//item')
        if item is None:
            return None, None, None

        # Nazwa (pierwsze <name type="primary">)
        name_elem = item.find(".//name[@type='primary']")
        name = name_elem.get('value') if name_elem is not None else None

        # Opis
        description_elem = item.find('description')
        description = description_elem.text.strip() if description_elem is not None else None

        # Statystyki
        stats = {}
        min_players = item.find('minplayers')
        max_players = item.find('maxplayers')
        min_playtime = item.find('minplaytime')
        max_playtime = item.find('maxplaytime')
        playingtime = item.find('playingtime')
        min_age = item.find('minage')
        stats['min_players'] = int(min_players.get('value')) if min_players is not None and min_players.get('value') else None
        stats['max_players'] = int(max_players.get('value')) if max_players is not None and max_players.get('value') else None
        stats['min_playtime'] = int(min_playtime.get('value')) if min_playtime is not None and min_playtime.get('value') else None
        stats['max_playtime'] = int(max_playtime.get('value')) if max_playtime is not None and max_playtime.get('value') else None
        stats['playtime'] = int(playingtime.get('value')) if playingtime is not None and playingtime.get('value') else None
        stats['min_age'] = int(min_age.get('value')) if min_age is not None and min_age.get('value') else None

        statistics = item.find('.//statistics')
        if statistics is not None:
            ratings = statistics.find('.//ratings')
            if ratings is not None:
                avg = ratings.find('average')
                if avg is not None:
                    stats['bgg_rating'] = float(avg.get('value')) if avg.get('value') else None
                usersrated = ratings.find('usersrated')
                if usersrated is not None:
                    stats['bgg_rating_count'] = int(usersrated.get('value')) if usersrated.get('value') else None
                avgweight = ratings.find('averageweight')
                if avgweight is not None:
                    stats['bgg_weight'] = float(avgweight.get('value')) if avgweight.get('value') else None
                numweights = ratings.find('numweights')
                if numweights is not None:
                    stats['bgg_weight_count'] = int(numweights.get('value')) if numweights.get('value') else None

        # Sugerowana liczba graczy
        suggested_players = None
        poll = item.find(".//poll[@name='suggested_numplayers']")
        if poll is not None:
            best = []
            recommended = []
            for results in poll.findall('results'):
                np = results.get('numplayers')
                if np is None:
                    continue
                best_votes = 0
                rec_votes = 0
                for result in results.findall('result'):
                    val = result.get('value')
                    votes = int(result.get('numvotes') or 0)
                    if val == 'Best':
                        best_votes += votes
                    elif val == 'Recommended':
                        rec_votes += votes
                if best_votes > 0 or rec_votes > 0:
                    best.append((np, best_votes))
                    recommended.append((np, rec_votes))
            if best:
                best_str = ", ".join([f"{np}" for np, v in sorted(best, key=lambda x: -x[1])[:3]])
                rec_str = ", ".join([f"{np}" for np, v in sorted(recommended, key=lambda x: -x[1])[:3]])
                suggested_players = f"Best: {best_str} | Recommended: {rec_str}"
        stats['suggested_players'] = suggested_players

        return name, description, stats

    except Exception as e:
        logging.error(f"Błąd przy {bgg_id}: {e}")
        return None, None, None

def save_game_to_db(bgg_id, name, description, stats):
    """Zapisuje dane gry do tabeli games."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        c.execute('''INSERT OR IGNORE INTO games
                     (bgg_id, name, description, min_players, max_players,
                      min_playtime, max_playtime, playtime, min_age,
                      bgg_rating, bgg_rating_count, bgg_weight, bgg_weight_count,
                      suggested_players, last_updated)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (bgg_id, name, description,
                   stats.get('min_players'), stats.get('max_players'),
                   stats.get('min_playtime'), stats.get('max_playtime'), stats.get('playtime'),
                   stats.get('min_age'), stats.get('bgg_rating'), stats.get('bgg_rating_count'),
                   stats.get('bgg_weight'), stats.get('bgg_weight_count'),
                   stats.get('suggested_players'),
                   datetime.datetime.now().isoformat()))
        conn.commit()
        logging.info(f"Zapisano grę {name} (ID {bgg_id})")
        return True
    except sqlite3.Error as e:
        logging.error(f"Błąd zapisu gry {bgg_id}: {e}")
        return False
    finally:
        conn.close()

def init_games_table():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS games (
        bgg_id TEXT PRIMARY KEY,
        name TEXT,
        description TEXT,
        min_players INT,
        max_players INT,
        min_playtime INT,
        max_playtime INT,
        playtime INT,
        min_age INT,
        bgg_rating REAL,
        bgg_rating_count INT,
        bgg_weight REAL,
        bgg_weight_count INT,
        suggested_players TEXT,
        last_updated TEXT
    )''')
    conn.commit()
    conn.close()
    logging.info("Tabela games jest gotowa.")

def wyslij_maila(temat, tresc):
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_USER
        msg['To'] = EMAIL_RECIPIENT
        msg['Subject'] = temat
        msg.attach(MIMEText(tresc, 'plain'))

        with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT) as server:
            server.login(EMAIL_USER, EMAIL_PASS)
            server.send_message(msg)
        logging.info("Mail wysłany pomyślnie")
    except Exception as e:
        logging.error(f"Błąd wysyłki maila: {e}")

def main():
    logging.info("--- Rozpoczynanie zbierania gier z BGG ---")
    init_games_table()

    # Odczytaj ostatnio przetworzone ID
    last_processed = 0
    if os.path.exists(LAST_ID_FILE):
        with open(LAST_ID_FILE, 'r') as f:
            try:
                last_processed = int(f.read().strip())
            except:
                pass

    start_id = last_processed + 1
    end_id = start_id + BATCH_SIZE - 1
    logging.info(f"Pobieranie gier od ID {start_id} do {end_id}")

    nowe_gry = 0
    for bgg_id in range(start_id, end_id + 1):
        # Sprawdź, czy gra już jest w bazie
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT 1 FROM games WHERE bgg_id = ?", (bgg_id,))
        exists = c.fetchone()
        conn.close()
        if exists:
            continue

        logging.info(f"Pobieranie ID {bgg_id}...")
        name, description, stats = fetch_game(bgg_id)
        if name and stats:
            if save_game_to_db(bgg_id, name, description, stats):
                nowe_gry += 1
        else:
            logging.warning(f"ID {bgg_id} nie zwróciło danych – pomijam")

        # Krótka przerwa co 50 gier (dodatkowe zabezpieczenie przed przeciążeniem API)
        if bgg_id % BATCH_SIZE == 0:
            time.sleep(5)

    # Zapisz nowy zakres
    with open(LAST_ID_FILE, 'w') as f:
        f.write(str(end_id))

    # Wyślij maila
    temat = f"BGG: pobrano dane {datetime.date.today()}"
    tresc = f"Pobrano {nowe_gry} nowych gier z przedziału ID {start_id}-{end_id}."
    wyslij_maila(temat, tresc)

    logging.info("--- Zakończono zbieranie gier ---")

if __name__ == "__main__":
    main()
