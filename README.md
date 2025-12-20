"""
Enterprise Feature Flag & Config Service
Single-file FastAPI application

Features:
- Feature flag creation & updates
- Environment targeting (dev/staging/prod)
- Percentage rollouts
- In-memory caching
- Cache invalidation
- SQLite persistence
- Production-grade backend pattern

Run:
pip install fastapi uvicorn pydantic
uvicorn app:app --reload
"""

import sqlite3
import random
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Feature Flag Service")
DB_PATH = "flags.db"

# -------------------------
# Database
# -------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS flags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            enabled INTEGER NOT NULL,
            rollout INTEGER NOT NULL,
            environment TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

init_db()

# -------------------------
# In-memory cache
# -------------------------
cache = {}

def invalidate_cache():
    cache.clear()

# -------------------------
# Models
# -------------------------
class FlagCreate(BaseModel):
    name: str
    enabled: bool
    rollout: int = 100  # percentage
    environment: str = "prod"

class FlagOut(BaseModel):
    name: str
    enabled: bool
    rollout: int
    environment: str
    active: bool

# -------------------------
# Helpers
# -------------------------
def get_db():
    return sqlite3.connect(DB_PATH)

def evaluate_flag(enabled: int, rollout: int) -> bool:
    if not enabled:
        return False
    return random.randint(1, 100) <= rollout

# -------------------------
# API Routes
# -------------------------
@app.post("/flags")
def create_or_update_flag(data: FlagCreate):
    if not (0 <= data.rollout <= 100):
        raise HTTPException(status_code=400, detail="Rollout must be 0-100")

    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO flags (name, enabled, rollout, environment, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            enabled=excluded.enabled,
            rollout=excluded.rollout,
            environment=excluded.environment,
            updated_at=excluded.updated_at
    """, (
        data.name,
        int(data.enabled),
        data.rollout,
        data.environment,
        datetime.utcnow().isoformat()
    ))
    conn.commit()
    conn.close()

    invalidate_cache()
    return {"message": "Flag saved"}

@app.get("/flags/{name}", response_model=FlagOut)
def get_flag(name: str, env: str = "prod"):
    cache_key = f"{name}:{env}"
    if cache_key in cache:
        return cache[cache_key]

    conn = get_db()
    cur = conn.cursor()
    row = cur.execute(
        "SELECT enabled, rollout, environment FROM flags WHERE name = ? AND environment = ?",
        (name, env)
    ).fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Flag not found")

    active = evaluate_flag(row[0], row[1])

    result = {
        "name": name,
        "enabled": bool(row[0]),
        "rollout": row[1],
        "environment": env,
        "active": active
    }

    cache[cache_key] = result
    return result

@app.get("/flags")
def list_flags():
    conn = get_db()
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT name, enabled, rollout, environment, updated_at FROM flags"
    ).fetchall()
    conn.close()

    return [
        {
            "name": r[0],
            "enabled": bool(r[1]),
            "rollout": r[2],
            "environment": r[3],
            "updated_at": r[4]
        }
        for r in rows
    ]
