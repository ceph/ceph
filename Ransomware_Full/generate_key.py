from Crypto.Random import get_random_bytes
import pickle

def generate_static_key():
    # Erzeuge einen statischen 128-Bit Schlüssel für die Entschlüsselung
    static_key = get_random_bytes(16)
    
    # Speichere den Schlüssel in einer Datei
    with open("static_key.pickle", "wb") as f:
        pickle.dump(static_key, f)

generate_static_key()
