import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


def _split(value):
    return [item.strip() for item in value.split(",") if item.strip()]


CONTACT_POINTS = _split(os.getenv("CASSANDRA_CONTACT_POINTS", "127.0.0.1"))
PORT = int(os.getenv("CASSANDRA_PORT") or "9042")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "pulse")
USERNAME = os.getenv("CASSANDRA_USERNAME") or None
PASSWORD = os.getenv("CASSANDRA_PASSWORD") or None
CONSISTENCY = os.getenv("CASSANDRA_CONSISTENCY", "QUORUM")
