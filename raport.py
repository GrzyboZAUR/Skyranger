#!/usr/bin/env python3
import os
import sqlite3
import subprocess
import smtplib
import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

# --- Wczytaj zmienne z .env (te same, co w innych skryptach) ---
load_dotenv('/home/grzybozaur/gamefound/.env')   # dostosuj ścieżkę jeśli trzeba
EMAIL_HOST = os.getenv('EMAIL_HOST')
EMAIL_PORT = int(os.getenv('EMAIL_PORT'))
EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_PASS = os.getenv('EMAIL_PASS')
EMAIL_RECIPIENT = os.getenv('EMAIL_RECIPIENT')

DB_FILE = "/home/grzybozaur/gamefound/gamefound.db"

def get_games_count():
    """Liczba gier w tabeli games (BGG)."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM games")
    count = c.fetchone()[0]
    conn.close()
    return count

def get_projects_count():
    """Liczba wierszy w tabeli projects (Gamefound)."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM projects")
    count = c.fetchone()[0]
    conn.close()
    return count

def get_db_size():
    """Rozmiar pliku bazy w MB."""
    if os.path.exists(DB_FILE):
        size_bytes = os.path.getsize(DB_FILE)
        return size_bytes / (1024 * 1024)
    return 0

def get_free_disk():
    """Wolne miejsce na dysku (w GB) dla partycji głównej."""
    stat = os.statvfs('/')
    free = stat.f_frsize * stat.f_bavail / (1024**3)
    return free

def get_cpu_temp():
    """Temperatura CPU (w stopniach C)."""
    try:
        temp = subprocess.check_output(['vcgencmd', 'measure_temp']).decode()
        return temp.strip().replace('temp=', '')
    except:
        return "N/A"

def send_email(subject, body):
    """Wysyła maila (identycznie jak w skryptach Gamefound/BGG)."""
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_USER
        msg['To'] = EMAIL_RECIPIENT
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT) as server:
            server.login(EMAIL_USER, EMAIL_PASS)
            server.send_message(msg)
        print("Mail wysłany pomyślnie.")
    except Exception as e:
        print(f"Błąd wysyłki maila: {e}")

def main():
    # Zbieramy dane
    games = get_games_count()
    projects = get_projects_count()
    db_size = get_db_size()
    free_disk = get_free_disk()
    temp = get_cpu_temp()
    date_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Przygotowanie treści (konsola i mail)
    report_lines = [
        f"📊 Raport z dnia: {date_str}",
        f"🎮 Gry w bazie (BGG): {games}",
        f"🚀 Projekty crowdfundingowe (Gamefound): {projects}",
        f"💾 Rozmiar bazy danych: {db_size:.2f} MB",
        f"💿 Wolne miejsce na karcie SD: {free_disk:.1f} GB",
        f"🌡️ Temperatura CPU: {temp}",
        "---",
        "Wszystkie systemy działają poprawnie."
    ]
    body = "\n".join(report_lines)

    # Wyświetlenie w konsoli
    print(body)

    # Wysłanie maila
    send_email(f"Status malinki - {date_str}", body)

if __name__ == "__main__":
    main()