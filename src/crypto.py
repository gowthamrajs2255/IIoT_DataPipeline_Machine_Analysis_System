from cryptography.fernet import Fernet,InvalidToken
import os

KEY = os.getenv("ENCRYPTION_KEY")

if KEY is None:
    raise ValueError("Encryption key not set")

cipher = Fernet(KEY.encode())


def encrypt(data: bytes) -> bytes:
    return cipher.encrypt(data)


def decrypt(data: bytes) -> bytes:
    try:
        return cipher.decrypt(data)
    except InvalidToken:
        raise ValueError("Message Authentication Failed")