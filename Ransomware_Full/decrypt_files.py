from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import os
import pickle

def load_static_key():
    # Lade den gespeicherten statischen Schlüssel
    with open("static_key.pickle", "rb") as f:
        static_key = pickle.load(f)
    return static_key

def decrypt_files(static_key):
    home_directory = os.path.expanduser("~")

    for root, directories, files in os.walk(home_directory):
        for filename in files:
            if filename.endswith('.encrypted'):
                file_path = os.path.join(root, filename)
                try:
                    with open(file_path, 'rb') as f:
                        cipher_text = f.read()

                    # Entschlüsselung des Dateiinhalts
                    cipher = AES.new(static_key, AES.MODE_CBC, iv=cipher_text[:16])
                    plain_text = unpad(cipher.decrypt(cipher_text[16:]), AES.block_size)

                    # Entferne das ".encrypted"-Suffix vom Dateinamen
                    decrypted_file_path = file_path[:-10]
                    with open(decrypted_file_path, 'wb') as f:
                        f.write(plain_text)

                    # Lösche die verschlüsselte Datei
                    os.remove(file_path)
                except Exception as e:
                    print(f"Fehler beim Entschlüsseln von {file_path}: {e}")

# Laden des statischen Schlüssels
static_key = load_static_key()

# Entschlüsseln der Dateien
decrypt_files(static_key)

# Lösche den statischen Schlüssel
os.remove("static_key.pickle")
        