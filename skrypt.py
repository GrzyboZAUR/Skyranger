import requests
import sqlite3
import datetime
import time
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv

# Wczytaj zmienne z pliku .env
load_dotenv()

# Pobierz zmienne środowiskowe
EMAIL_HOST = os.getenv('EMAIL_HOST')
EMAIL_PORT = int(os.getenv('EMAIL_PORT'))
EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_PASS = os.getenv('EMAIL_PASS')
EMAIL_RECIPIENT = os.getenv('EMAIL_RECIPIENT')

def wyslij_maila(temat, tresc):
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_USER
        msg['To'] = "grzybas@gmail.com"
        msg['Subject'] = temat
        msg.attach(MIMEText(tresc, 'plain'))

        with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT) as server:
            server.login(EMAIL_USER, EMAIL_PASS)
            server.send_message(msg)
        print("Mail wysłany pomyślnie")
    except Exception as e:
        print(f"Błąd wysyłki maila: {type(e).__name__}: {e}")

# --- Konfiguracja ---
DB_FILE = "gamefound.db"
API_ACTIVE = "https://gamefound.com/api/public/projects/getActiveCrowdfundingProjects"
API_PROJECT = "https://gamefound.com/api/public/projects/getCrowdfundingProject"
API_CREATOR = "https://gamefound.com/api/public/creators/getCreator"

# Ustaw logowanie (zobaczymy co się dzieje)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Sesja z retry (na wypadek błędów sieci)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))


def init_database():
    """Tworzy tabele, jeśli nie istnieją (rozszerzona wersja)."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Główna tabela projektów (już istnieje, ale dodajemy jeśli nie ma)
    c.execute('''CREATE TABLE IF NOT EXISTS projects
    (
        date
        TEXT,
        urlName
        TEXT,
        projectName
        TEXT,
        backerCount
        INT,
        fundsGathered
        INT,
        campaignGoal
        INT,
        campaignEndDate
        TEXT,
        PRIMARY
        KEY
                 (
        date,
        urlName
                 ))''')

    # Nowa tabela szczegółów
    c.execute('''CREATE TABLE IF NOT EXISTS project_details
    (
        date
        TEXT,
        urlName
        TEXT,
        updateCount
        INT,
        rewardCount
        INT,
        campaignStartDate
        TEXT,
        creatorName
        TEXT,
        creatorUrlName
        TEXT,
        currencyShortName
        TEXT,
        shortDescription
        TEXT,
        commentCount
        INT,
        projectHomeUrl
        TEXT,
        projectImageUrl
        TEXT,
        PRIMARY
        KEY
                 (
        date,
        urlName
                 ))''')

    # Tabela twórców
    c.execute('''CREATE TABLE IF NOT EXISTS creators
                 (
                     urlName
                     TEXT
                     PRIMARY
                     KEY,
                     name
                     TEXT,
                     description
                     TEXT,
                     thumbImageUrl
                     TEXT,
                     creatorPageUrl
                     TEXT,
                     lastUpdated
                     TEXT
                 )''')

    conn.commit()
    conn.close()
    logging.info("Baza danych gotowa.")


def fetch_active_projects():
    """Pobiera listę aktywnych projektów (bez zmian)."""
    try:
        response = session.get(API_ACTIVE, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Błąd pobierania listy: {e}")
        return None


def fetch_project_details(urlName):
    """Pobiera szczegóły pojedynczego projektu."""
    try:
        url = f"{API_PROJECT}?urlName={urlName}"
        response = session.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Błąd pobierania {urlName}: {e}")
        return None


def fetch_creator(creatorUrlName):
    """Pobiera dane twórcy."""
    if not creatorUrlName:
        return None
    try:
        url = f"{API_CREATOR}?urlName={creatorUrlName}"
        response = session.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Błąd pobierania twórcy {creatorUrlName}: {e}")
        return None


def save_project(project, date):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    data_tuple = (
        date,
        project.get('projectUrlName'),
        project.get('projectName'),
        project.get('backerCount'),
        project.get('fundsGathered'),
        project.get('campaignGoal'),
        project.get('campaignEndDate')
    )
    try:
        c.execute('''INSERT OR IGNORE INTO projects
                     (date, urlName, projectName, backerCount, fundsGathered, campaignGoal, campaignEndDate)
                     VALUES (?, ?, ?, ?, ?, ?, ?)''', data_tuple)
    except sqlite3.Error as e:
        logging.error(f"Błąd zapisu projektu {project.get('projectName')}: {e}")
    conn.commit()
    conn.close()


def save_project_details(details, date):
    """Zapisuje szczegóły projektu."""
    if not details:
        return
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    data_tuple = (
        date,
        details.get('projectUrlName'),
        details.get('updateCount'),
        details.get('rewardCount'),
        details.get('campaignStartDate'),
        details.get('creatorName'),
        details.get('creatorUrlName'),
        details.get('currencyShortName'),
        details.get('shortDescription'),
        details.get('commentCount'),
        details.get('projectHomeUrl'),
        details.get('projectImageUrl')
    )
    try:
        c.execute('''INSERT
        OR IGNORE INTO project_details
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', data_tuple)
    except sqlite3.Error as e:
        logging.error(f"Błąd zapisu szczegółów: {e}")
    conn.commit()
    conn.close()


def save_creator(creator_data):
    """Zapisuje lub aktualizuje dane twórcy."""
    if not creator_data:
        return
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    data_tuple = (
        creator_data.get('urlName'),
        creator_data.get('name'),
        creator_data.get('description'),
        creator_data.get('thumbImageUrl'),
        creator_data.get('creatorPageUrl'),
        datetime.datetime.now().isoformat()
    )
    try:
        c.execute('''INSERT OR REPLACE INTO creators
                     VALUES (?, ?, ?, ?, ?, ?)''', data_tuple)
    except sqlite3.Error as e:
        logging.error(f"Błąd zapisu twórcy: {e}")
    conn.commit()
    conn.close()


def main():
    today = datetime.date.today().isoformat()
    logging.info(f"--- Rozpoczynanie zbierania danych: {datetime.datetime.now()} ---")

    init_database()

    # 1. Pobierz listę aktywnych projektów
    active_projects = fetch_active_projects()
    if not active_projects:
        logging.error("Brak danych – kończę.")
        return

    logging.info(f"Pobrano {len(active_projects)} aktywnych projektów.")

    # 2. Dla każdego projektu pobierz szczegóły
    for idx, proj in enumerate(active_projects):
        urlName = proj.get('projectUrlName')
        if not urlName:
            continue

        logging.info(f"({idx + 1}/{len(active_projects)}) Przetwarzanie: {urlName}")

        # Zapisz podstawowe dane
        save_project(proj, today)

        # Pobierz szczegóły
        details = fetch_project_details(urlName)
        if details:
            save_project_details(details, today)

            # 3. Jeśli jest twórca, pobierz/go (opcjonalnie)
            creator_url = details.get('creatorUrlName')
            if creator_url:
                # Sprawdź, czy twórca już istnieje (możesz dodać logikę odświeżania co X dni)
                creator = fetch_creator(creator_url)
                if creator:
                    save_creator(creator)

        # Grzecznościowa przerwa – nie atakuj API
        time.sleep(1)  # 1 sekundy między projektami

    temat = f"Gamefound: dane z {datetime.date.today()}"
    tresc = f"Pobrano {len(active_projects)} aktywnych projektów.\nBaza danych zaktualizowana."
    wyslij_maila(temat, tresc)

    logging.info("--- Zakończono ---")


if __name__ == "__main__":
    main()
