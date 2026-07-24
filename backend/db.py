from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

from config import (
    CONSISTENCY,
    CONTACT_POINTS,
    KEYSPACE,
    PASSWORD,
    PORT,
    USERNAME,
)


def _auth_provider():
    if USERNAME and PASSWORD:
        return PlainTextAuthProvider(username=USERNAME, password=PASSWORD)
    return None


def _consistency_level(name):
    return getattr(ConsistencyLevel, name.upper(), ConsistencyLevel.QUORUM)


def connect(keyspace=KEYSPACE, consistency=None):
    cluster = Cluster(
        contact_points=CONTACT_POINTS,
        port=PORT,
        auth_provider=_auth_provider(),
    )
    session = cluster.connect(keyspace) if keyspace else cluster.connect()
    session.default_consistency_level = consistency or _consistency_level(CONSISTENCY)
    return cluster, session
