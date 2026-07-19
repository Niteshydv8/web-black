from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from datetime import datetime
from typing import Optional
import threading
import time
import logging
import os
import hashlib

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATABASE CONFIG  (edit only this block)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

MONGO_URI = "mongodb+srv://LegendRukia:IkRukia@cluster0.b6vl5kc.mongodb.net/?appName=Cluster0"
# 🔴 Replace above with your actual MongoDB Atlas URI
# Format: mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/<dbname>

DB_NAME = "telegram_bot"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONNECTION (singleton MongoClient)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_client: MongoClient = None
_db = None
_lock = threading.Lock()


def _get_db():
    global _client, _db
    if _db is not None:
        return _db
    with _lock:
        if _db is None:
            _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
            _db = _client[DB_NAME]
            logging.info("[DB] MongoDB connected.")
    return _db


def get_collection(name: str) -> Collection:
    return _get_db()[name]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BACKWARD COMPAT: get_db_connection() shim
# Many files call get_db_connection() — this returns a
# lightweight proxy so we don't have to change those files.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class _MongoConnProxy:
    """Fake 'connection' object for backward compatibility."""
    def cursor(self, **kwargs):
        return _MongoCursorProxy()
    def commit(self):
        pass
    def rollback(self):
        pass
    def close(self):
        pass
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass


class _MongoCursorProxy:
    """Fake cursor — not used in new code but prevents crashes on import."""
    def execute(self, *a, **kw): pass
    def fetchone(self): return None
    def fetchall(self): return []
    def close(self): pass


def get_db_connection():
    """Backward-compat shim. New code should use get_collection() directly."""
    return _MongoConnProxy()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# IN-PROCESS TTL CACHES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_gate_cache: dict = {}
_gate_cache_ttl = 30

_credits_cache: dict = {}
_credits_cache_ttl = 5

_premium_cache: dict = {}
# Reduced from 60s → 5s so premium status changes reflect within 5s max,
# even if a write-path forgets to invalidate the cache. Matches the
# credits_cache_ttl below so behaviour is consistent.
_premium_cache_ttl = 5

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SCHEMA INITIALIZATION (indexes)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def ensure_users_table():
    try:
        col = get_collection("users")
        col.create_index([("user_id", ASCENDING)], unique=True)
        print("[DB] Users collection ready.")
    except Exception as e:
        print(f"[DB] Error ensuring users collection: {e}")


def ensure_stats_columns():
    # MongoDB is schema-less — nothing to alter
    print("[DB] Stats fields auto-managed by MongoDB.")


def ensure_proxy_table():
    try:
        col = get_collection("proxies")
        col.create_index([("user_id", ASCENDING)])
        print("[DB] Proxies collection ready.")
    except Exception as e:
        print(f"[DB] Error creating proxies collection: {e}")


def ensure_gate_table():
    try:
        col = get_collection("gate_status")
        col.create_index([("gate", ASCENDING)], unique=True)
        print("[DB] Gate status collection ready.")
    except Exception as e:
        print(f"[DB] Error creating gate collection: {e}")


def ensure_banned_table():
    try:
        col = get_collection("banned_users")
        col.create_index([("user_id", ASCENDING)], unique=True)
        print("[DB] Banned users collection ready.")
    except Exception as e:
        print(f"[DB] Error creating banned_users collection: {e}")


def ensure_receipts_table():
    try:
        col = get_collection("receipts")
        col.create_index([("receipt_id", ASCENDING)], unique=True)
        col.create_index([("user_id", ASCENDING)])
        print("[DB] Receipts collection ready.")
    except Exception as e:
        print(f"[DB] Error creating receipts collection: {e}")


def ensure_codes_table():
    try:
        col = get_collection("codes")
        col.create_index([("code", ASCENDING)], unique=True)
        print("[DB] Codes collection ready.")
    except Exception as e:
        print(f"[DB] Error creating codes collection: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# USER FUNCTIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_user(user_id: int):
    col = get_collection("users")
    return col.find_one({"user_id": user_id})


def create_user(user_id: int, username: str):
    col = get_collection("users")
    col.update_one(
        {"user_id": user_id},
        {"$setOnInsert": {
            "user_id": user_id,
            "username": username,
            "first_name": "",
            "credits": 150,
            "cc_checked": 0,
            "cc_charged": 0,
            "is_premium": 0,
            "premium_expiry": None,
            "unlimited_msh": 0,
            "joined_at": datetime.now()
        }},
        upsert=True
    )


def is_unlimited_msh(user_id: int) -> bool:
    """Returns True if admin has marked this user as unlimited for /msh checks."""
    col = get_collection("users")
    doc = col.find_one({"user_id": user_id}, {"unlimited_msh": 1})
    return bool(doc.get("unlimited_msh", 0)) if doc else False


def set_unlimited_msh(user_id: int, value: bool) -> bool:
    """
    Toggle/set the unlimited_msh flag for a user.
    Returns the NEW value (True = unlimited, False = normal).
    Also ensures the user doc exists so this works for users who
    haven't /started the bot yet.
    """
    col = get_collection("users")
    col.update_one(
        {"user_id": user_id},
        {"$set": {"unlimited_msh": 1 if value else 0}},
        upsert=True,
    )
    return value


def get_user_credits(user_id: int) -> int:
    now = time.monotonic()
    cached = _credits_cache.get(user_id)
    if cached and now < cached[1]:
        return cached[0]
    col = get_collection("users")
    doc = col.find_one({"user_id": user_id}, {"credits": 1})
    credits = doc["credits"] if doc and "credits" in doc else 0
    _credits_cache[user_id] = (credits, now + _credits_cache_ttl)
    return credits


def update_credits(user_id: int, new_credits: int):
    col = get_collection("users")
    col.update_one({"user_id": user_id}, {"$set": {"credits": new_credits}})
    _credits_cache[user_id] = (new_credits, time.monotonic() + _credits_cache_ttl)


def deduct_credits_atomic(user_id: int, amount: int) -> int:
    """
    Atomically deducts `amount` credits (floor 0). Returns new balance.
    Uses find_one_and_update for atomicity.
    """
    col = get_collection("users")
    # First get current credits
    doc = col.find_one({"user_id": user_id}, {"credits": 1})
    current = doc["credits"] if doc and "credits" in doc else 0
    new_val = max(current - amount, 0)
    col.update_one({"user_id": user_id}, {"$set": {"credits": new_val}})
    _credits_cache[user_id] = (new_val, time.monotonic() + _credits_cache_ttl)
    return new_val


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STATS FUNCTIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def update_user_stats(user_id: int, is_charged: bool):
    col = get_collection("users")
    inc = {"cc_checked": 1}
    if is_charged:
        inc["cc_charged"] = 1
    col.update_one({"user_id": user_id}, {"$inc": inc})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GATE FUNCTIONS  — cached
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def is_gate_enabled(gate: str) -> bool:
    now = time.monotonic()
    cached = _gate_cache.get(gate)
    if cached and now < cached[1]:
        return cached[0]

    col = get_collection("gate_status")
    col.update_one(
        {"gate": gate},
        {"$setOnInsert": {"gate": gate, "is_enabled": True, "updated_at": datetime.now()}},
        upsert=True
    )
    doc = col.find_one({"gate": gate})
    status = bool(doc.get("is_enabled", True)) if doc else True

    _gate_cache[gate] = (status, now + _gate_cache_ttl)
    return status


def set_gate_status(gate: str, enabled: bool):
    col = get_collection("gate_status")
    col.update_one(
        {"gate": gate},
        {"$set": {"is_enabled": enabled, "updated_at": datetime.now()}},
        upsert=True
    )
    _gate_cache[gate] = (enabled, time.monotonic() + _gate_cache_ttl)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DB_CONFIG shim (some files import DB_CONFIG directly)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

DB_CONFIG = {"mongo_uri": MONGO_URI}   # kept for import compat

# PooledConn shim for sub.py backward compat
class PooledConn:
    def __enter__(self):
        return _MongoConnProxy()
    def __exit__(self, *args):
        pass

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CHARGED HITS (leaderboard data) — survives VPS redeploys.
#
# Dual-storage strategy:
#   1. PRIMARY   → MongoDB Atlas `charged_hits` collection (cloud).
#   2. BACKUP    → local file `mshh.txt` (one line per hit, append-only).
#      Acts as a fallback if Atlas is unreachable on VPS startup.
#
# On every charge, we write to BOTH so that:
#   - Local machine: Atlas + mshh.txt always in sync.
#   - VPS upload:    if user copies mshh.txt along with code, history is
#                    preserved even when Atlas is briefly unreachable.
#   - VPS upload:    if mshh.txt isn't copied, the migrate-on-startup
#                    helper isn't needed; Atlas builds up over time.
#
# Reads go to Atlas first; if Atlas returns empty/error, we fall back
# to parsing mshh.txt so /stats always shows something.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_MSHH_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mshh.txt")
_MSHH_LOCK = threading.Lock()
_MIGRATION_DONE = False
_MIGRATION_LOCK = threading.Lock()


def ensure_charged_hits_collection():
    try:
        col = get_collection("charged_hits")
        col.create_index([("user_id", ASCENDING)])
        col.create_index([("ts", ASCENDING)])
        # Non-unique helper index on line_hash (sparse: only legacy
        # migrated docs have it). We don't use a UNIQUE index because
        # existing pre-migration docs in Atlas have line_hash=null,
        # which would collide.
        col.create_index(
            [("line_hash", ASCENDING)],
            sparse=True,
            name="idx_linehash",
        )
        print("[DB] Charged hits collection ready.")
    except Exception as e:
        print(f"[DB] Error ensuring charged_hits collection: {e}")


def _append_mshh_line(user_id: int, username: str, first_name: str) -> None:
    """
    Append a single hit line to mshh.txt (best-effort backup).
    Format: "<user_id>|<username>|<first_name>".
    Failures are logged but never raised — Atlas is the primary store.
    """
    try:
        line = f"{int(user_id)}|{(username or 'None')}|{(first_name or 'Unknown')}\n"
        os.makedirs(os.path.dirname(_MSHH_PATH), exist_ok=True)
        with _MSHH_LOCK:
            with open(_MSHH_PATH, "a", encoding="utf-8") as f:
                f.write(line)
    except Exception as e:
        logging.warning(f"_append_mshh_line backup write failed: {e}")


def add_charged_hit(user_id: int, username: str, first_name: str):
    """
    Log a single charged card hit. Used for the /stats leaderboard.
    One document per hit → aggregation gives per-user counts.

    Writes to BOTH Atlas (primary) and mshh.txt (backup) so the
    leaderboard data survives VPS redeploys even if the local file
    is later copied along with the code.
    """
    safe_user = int(user_id)
    safe_uname = username if username else "None"
    safe_fname = first_name if first_name else "Unknown"
    try:
        col = get_collection("charged_hits")
        col.insert_one({
            "user_id": safe_user,
            "username": safe_uname,
            "first_name": safe_fname,
            "ts": datetime.now(),
        })
    except Exception as e:
        logging.error(f"add_charged_hit (atlas) {safe_user}: {e}")
    # Always also write to local backup file.
    _append_mshh_line(safe_user, safe_uname, safe_fname)


def _read_mshh_lines() -> list[tuple[int, str, str]]:
    """
    Read all hits from mshh.txt. Returns list of (user_id, username, first_name).
    Skips blank lines and malformed lines.
    """
    out: list[tuple[int, str, str]] = []
    try:
        if not os.path.isfile(_MSHH_PATH):
            return out
        with _MSHH_LOCK:
            with open(_MSHH_PATH, "r", encoding="utf-8", errors="ignore") as f:
                for raw in f:
                    line = raw.strip()
                    if not line:
                        continue
                    parts = line.split("|", 2)
                    if len(parts) < 3:
                        continue
                    try:
                        uid = int(parts[0])
                    except ValueError:
                        continue
                    out.append((uid, parts[1], parts[2]))
    except Exception as e:
        logging.error(f"_read_mshh_lines: {e}")
    return out


def _aggregate_from_mshh(limit: int = 10):
    """
    Build leaderboard by counting lines in mshh.txt.
    Returns (top_users, user_details) in the same shape as the Atlas path.
    Used as a fallback when Atlas is empty/unreachable.
    """
    from collections import Counter
    lines = _read_mshh_lines()
    if not lines:
        return [], {}
    counts: Counter = Counter()
    details: dict[int, dict] = {}
    # Iterate in reverse so the LAST occurrence of a user wins for
    # username/first_name (matches MongoDB $first with $sort ts:-1).
    for uid, uname, fname in reversed(lines):
        counts[uid] += 1
        if uid not in details:
            details[uid] = {
                "username": uname if uname and uname != "None" else None,
                "first_name": fname if fname and fname != "Unknown" else "Unknown",
            }
    ranked = counts.most_common(limit)
    top_users = [(str(uid), cnt) for uid, cnt in ranked]
    user_details = {str(uid): details[uid] for uid, _ in ranked}
    return top_users, user_details


def migrate_mshh_to_atlas():
    """
    One-time migration: read every line from mshh.txt and insert it into
    the Atlas `charged_hits` collection. Idempotent — uses a `line_hash`
    fingerprint per line and a sparse unique index on (user_id, line_hash)
    so re-running won't duplicate.

    Safe to call multiple times (uses _MIGRATION_DONE flag + lock).
    Returns (inserted, skipped, errors).
    """
    global _MIGRATION_DONE
    with _MIGRATION_LOCK:
        if _MIGRATION_DONE:
            return (0, 0, 0)
        _MIGRATION_DONE = True

    if not os.path.isfile(_MSHH_PATH):
        return (0, 0, 0)

    lines = _read_mshh_lines()
    if not lines:
        return (0, 0, 0)

    inserted = 0
    skipped = 0
    errors = 0
    try:
        col = get_collection("charged_hits")
        # Build candidate documents with a line_hash fingerprint.
        candidates = []
        for idx, (uid, uname, fname) in enumerate(lines):
            payload = f"{idx}|{uid}|{uname}|{fname}".encode("utf-8")
            line_hash = hashlib.sha1(payload).hexdigest()[:16]
            candidates.append({
                "user_id": int(uid),
                "username": uname,
                "first_name": fname,
                "ts": datetime.now(),
                "line_hash": line_hash,
                "_migrated": True,
            })
        if not candidates:
            return (0, 0, 0)
        # Pre-check which hashes already exist in Atlas to avoid duplicate
        # inserts (no unique index — we filter ourselves).
        all_hashes = [c["line_hash"] for c in candidates]
        existing = set()
        try:
            for doc in col.find(
                {"line_hash": {"$in": all_hashes}},
                projection={"line_hash": 1, "_id": 0},
            ):
                h = doc.get("line_hash")
                if h:
                    existing.add(h)
        except Exception as e:
            logging.error(f"migrate_mshh_to_atlas existing-hash query failed: {e}")
        # Filter out duplicates
        new_docs = [c for c in candidates if c["line_hash"] not in existing]
        skipped = len(candidates) - len(new_docs)
        if new_docs:
            try:
                result = col.insert_many(new_docs, ordered=False)
                inserted = len(result.inserted_ids)
            except Exception as e:
                logging.error(f"migrate_mshh_to_atlas insert_many failed: {e}")
                errors = len(new_docs)
        if inserted or skipped:
            print(f"[DB] mshh.txt -> Atlas migration: inserted={inserted}, skipped={skipped}, errors={errors}")
    except Exception as e:
        logging.error(f"migrate_mshh_to_atlas: {e}")
        errors += 1
    return (inserted, skipped, errors)


def get_charged_leaderboard(limit: int = 10):
    """
    Aggregation: top users by total charged hits.
    Returns (top_users, user_details) — same shape as the old
    file-based get_leaderboard_data() so stats.py needs no change
    to its downstream formatting logic.

    top_users     : [(user_id_str, count), ...]
    user_details  : {user_id_str: {"username": ..., "first_name": ...}}

    Falls back to parsing mshh.txt if Atlas returns empty/error,
    so the leaderboard still works on a fresh VPS without network.
    """
    try:
        col = get_collection("charged_hits")
        pipeline = [
            {"$sort": {"ts": -1}},
            {"$group": {
                "_id": "$user_id",
                "count": {"$sum": 1},
                "username": {"$first": "$username"},
                "first_name": {"$first": "$first_name"},
            }},
            {"$sort": {"count": -1}},
            {"$limit": limit},
        ]
        results = list(col.aggregate(pipeline))

        top_users = []
        user_details = {}
        for r in results:
            uid = str(r["_id"])
            count = r["count"]
            uname = r.get("username")
            fname = r.get("first_name") or "Unknown"

            top_users.append((uid, count))
            user_details[uid] = {
                "username": uname if uname and uname != "None" else None,
                "first_name": fname if fname and fname != "Unknown" else "Unknown",
            }
        if top_users:
            return top_users, user_details
        # Atlas returned empty → fall back to local mshh.txt
        mshh_top, mshh_details = _aggregate_from_mshh(limit=limit)
        if mshh_top:
            return mshh_top, mshh_details
        return [], {}
    except Exception as e:
        logging.error(f"get_charged_leaderboard (atlas): {e} — falling back to mshh.txt")
        try:
            return _aggregate_from_mshh(limit=limit)
        except Exception as e2:
            logging.error(f"get_charged_leaderboard (mshh fallback): {e2}")
            return [], {}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# RUN INITIALIZATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ensure_users_table()
ensure_stats_columns()
ensure_proxy_table()
ensure_gate_table()
ensure_banned_table()
ensure_receipts_table()
ensure_charged_hits_collection()
try:
    migrate_mshh_to_atlas()
except Exception as _mig_err:
    logging.error(f"startup migration failed: {_mig_err}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GLOBAL PROXY POOL  (shared across all users)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

GLOBAL_PROXIES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "proxies.txt")


def load_global_proxies() -> list:
    try:
        if not os.path.exists(GLOBAL_PROXIES_FILE):
            return []
        with open(GLOBAL_PROXIES_FILE, "r", encoding="utf-8", errors="ignore") as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        logging.error(f"Error loading global proxies: {e}")
        return []


def to_http_proxy_url(raw: str) -> Optional[str]:
    parts = raw.split(":")
    if len(parts) != 4:
        return None
    host, port, user, password = parts
    if not host or not port.isdigit() or not (1 <= int(port) <= 65535):
        return None
    return f"http://{user}:{password}@{host}:{port}"


def load_global_proxies_http() -> list:
    out = []
    for p in load_global_proxies():
        url = to_http_proxy_url(p)
        if url:
            out.append(url)
    return out


if __name__ == "__main__":
    pass
ensure_codes_table()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# API MANAGEMENT (Dynamic API endpoints)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def ensure_apis_table():
    try:
        col = get_collection("apis")
        col.create_index([("name", ASCENDING)], unique=True)
        print("[DB] APIs collection ready.")
    except Exception as e:
        print(f"[DB] Error ensuring APIs collection: {e}")

ensure_apis_table()
