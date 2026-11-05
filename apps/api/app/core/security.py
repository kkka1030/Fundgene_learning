import hashlib
import hmac
import secrets
from base64 import urlsafe_b64decode, urlsafe_b64encode

PBKDF2_ITERATIONS = 600_000
PASSWORD_SCHEME = "pbkdf2_sha256"


def _encode_bytes(value: bytes) -> str:
    return urlsafe_b64encode(value).decode("utf-8")


def _decode_bytes(value: str) -> bytes:
    return urlsafe_b64decode(value.encode("utf-8"))


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    derived = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PBKDF2_ITERATIONS,
    )
    return (
        f"{PASSWORD_SCHEME}${PBKDF2_ITERATIONS}"
        f"${_encode_bytes(salt)}${_encode_bytes(derived)}"
    )


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        scheme, iterations_raw, salt_raw, digest_raw = stored_hash.split("$", 3)
    except ValueError:
        return False

    if scheme != PASSWORD_SCHEME:
        return False

    try:
        iterations = int(iterations_raw)
    except ValueError:
        return False

    salt = _decode_bytes(salt_raw)
    expected_digest = _decode_bytes(digest_raw)
    actual_digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        iterations,
    )
    return hmac.compare_digest(actual_digest, expected_digest)


def create_session_token() -> str:
    return secrets.token_urlsafe(32)


def hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()
