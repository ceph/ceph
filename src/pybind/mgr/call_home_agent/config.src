import os
import sys

from cryptography.fernet import Fernet, InvalidToken

defaultKeyFile = ''
defaultKey = b''
settings = {
    'api_key': b'',
    'private_key': b''
}

sys.tracebacklimit = 0

def get_settings() -> dict:
    key = _load_key()
    try:
        d = {k: _decrypt(key, v) for k, v in settings.items()}
        return d
    except InvalidToken:
        raise Exception("Settings encrypted with a different token. A new settings dict must be generated with the new token")
    except Exception as ex:
        raise Exception("Error getting encrypted settings: %s", (ex))

def generate_settings(source: dict) -> dict:
    key = _load_key()
    return {k: _encrypt(key, v) for k, v in source.items()}

def _decrypt(key: bytes, value: bytes) -> bytes:
    return Fernet(key).decrypt(value)

def _encrypt(key: bytes, value: str) -> bytes:
    return Fernet(key).encrypt(value.encode())

def _write_key() -> None:
    if os.path.isfile(defaultKeyFile):
        raise Exception("Not allowed to generate a new key file when a previous one exists")
    else:
        with open(defaultKeyFile, "wb") as key_file:
            key_file.write(Fernet.generate_key())

def _load_key() -> bytes:
    if os.path.isfile(defaultKeyFile):
        with open(defaultKeyFile, 'rb') as f:
            return f.read()
    else:
        return defaultKey
