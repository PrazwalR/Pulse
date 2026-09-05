"""PULSE — live fraud-alert dashboard (Streamlit, read-only).

A thin window into the Cassandra `pulse` keyspace: it reads the alerts the Go
rule engine writes and never touches the write path. Config comes from the same
.env / environment the backend uses.

Run:  streamlit run app.py
"""

import os
from collections import Counter

import streamlit as st
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

try:
    from dotenv import load_dotenv

    load_dotenv()
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
except ImportError:
    pass


def _split(value):
    return [item.strip() for item in value.split(",") if item.strip()]


CONTACT_POINTS = _split(os.getenv("CASSANDRA_CONTACT_POINTS", "127.0.0.1"))
PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "pulse")
USERNAME = os.getenv("CASSANDRA_USERNAME") or None
PASSWORD = os.getenv("CASSANDRA_PASSWORD") or None

SEVERITY_ICON = {"high": "🔴", "medium": "🟠", "low": "⚪"}


@st.cache_resource
def get_session():
    auth = PlainTextAuthProvider(USERNAME, PASSWORD) if USERNAME and PASSWORD else None
    cluster = Cluster(CONTACT_POINTS, port=PORT, auth_provider=auth)
    session = cluster.connect(KEYSPACE)
    session.row_factory = dict_factory
    # PER PARTITION LIMIT grabs a recent slice from each account without scanning
    # every row — fine for a read-only demo dashboard (not the hot path).
    prepared = session.prepare(
        "SELECT account_id, raised_at, rule, severity, detail "
        "FROM alerts_by_account PER PARTITION LIMIT 5"
    )
    return session, prepared


def load_alerts(limit=200):
    session, prepared = get_session()
    rows = list(session.execute(prepared))
    rows.sort(key=lambda r: r["raised_at"], reverse=True)
    return rows[:limit]


st.set_page_config(page_title="PULSE", layout="wide")
st.title("PULSE — live fraud alerts")
st.caption(f"keyspace `{KEYSPACE}` · alerts_by_account")

try:
    alerts = load_alerts()
except Exception as exc:  # cluster down, wrong keyspace, schema not applied, ...
    st.error(
        "Could not read alerts from Cassandra.\n\n"
        f"`{type(exc).__name__}: {exc}`\n\n"
        "Check the cluster is up (`docker compose -f extra/docker-compose.yml ps`), "
        "the schema is applied, and CASSANDRA_* settings are correct."
    )
    st.stop()

if not alerts:
    st.info("No alerts yet. Start the producer: `go run ./cmd/producer`.")
else:
    left, right = st.columns([1, 2])
    with left:
        st.subheader("Alerts by rule")
        st.bar_chart(dict(Counter(a["rule"] for a in alerts)))
        st.metric("Alerts shown", len(alerts))
    with right:
        st.subheader("Recent alerts")
        for a in alerts[:60]:
            icon = SEVERITY_ICON.get(a["severity"], "⚪")
            st.write(
                "%s **%s** — %s — %s — %s"
                % (icon, a["account_id"], a["rule"], a["detail"], a["raised_at"])
            )

if st.button("Refresh"):
    st.rerun()
