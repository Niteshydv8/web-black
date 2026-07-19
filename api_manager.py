"""
API Management System
Allows admins to dynamically manage API endpoints from the bot
Stores in MongoDB, performs health checks
"""

import os
import time
import logging
import asyncio
from datetime import datetime
from database import get_collection

try:
    import httpx
except ImportError:
    httpx = None

async def health_check_api(endpoint: str, timeout: int = 5) -> dict:
    """
    Ping API endpoint, return health status
    Returns:
    {
        "status": "UP" or "DOWN",
        "response_time_ms": int,
        "status_code": int or None,
        "error": str or None
    }
    """
    if not httpx:
        logging.warning("httpx not available, skipping health check")
        return {"status": "UNKNOWN", "response_time_ms": 0, "status_code": None, "error": "httpx not available"}
    
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            start = time.time()
            response = await client.get(endpoint)
            elapsed_ms = int((time.time() - start) * 1000)
            return {
                "status": "UP" if 200 <= response.status_code < 300 else "DOWN",
                "response_time_ms": elapsed_ms,
                "status_code": response.status_code,
                "error": None
            }
    except asyncio.TimeoutError:
        return {
            "status": "DOWN",
            "response_time_ms": timeout * 1000,
            "status_code": None,
            "error": "Timeout"
        }
    except Exception as e:
        return {
            "status": "DOWN",
            "response_time_ms": 0,
            "status_code": None,
            "error": str(e)
        }

async def add_api(name: str, endpoint: str) -> bool:
    """Add new API to MongoDB"""
    try:
        col = get_collection("apis")
        health = await health_check_api(endpoint)
        col.insert_one({
            "name": name.lower(),
            "endpoint": endpoint,
            "status": health["status"],
            "response_time_ms": health["response_time_ms"],
            "status_code": health.get("status_code"),
            "created_at": datetime.now(),
            "last_checked": datetime.now(),
            "error": health.get("error")
        })
        logging.info(f"API '{name}' added: {endpoint}")
        return True
    except Exception as e:
        logging.error(f"add_api error: {e}")
        return False

async def remove_api(name: str) -> bool:
    """Remove API from MongoDB"""
    try:
        col = get_collection("apis")
        result = col.delete_one({"name": name.lower()})
        logging.info(f"API '{name}' removed")
        return result.deleted_count > 0
    except Exception as e:
        logging.error(f"remove_api error: {e}")
        return False

async def get_all_apis() -> list:
    """Get all APIs from MongoDB"""
    try:
        col = get_collection("apis")
        return list(col.find({}, {"_id": 0}))
    except Exception as e:
        logging.error(f"get_all_apis error: {e}")
        return []

async def get_api(name: str) -> dict:
    """Get single API by name"""
    try:
        col = get_collection("apis")
        return col.find_one({"name": name.lower()}, {"_id": 0})
    except Exception as e:
        logging.error(f"get_api error: {e}")
        return None

async def check_and_update_api(name: str) -> dict:
    """Health check API and update status in MongoDB"""
    try:
        col = get_collection("apis")
        api_doc = col.find_one({"name": name.lower()})
        if not api_doc:
            return None
        
        health = await health_check_api(api_doc["endpoint"])
        col.update_one(
            {"name": name.lower()},
            {"$set": {
                "status": health["status"],
                "response_time_ms": health["response_time_ms"],
                "status_code": health.get("status_code"),
                "last_checked": datetime.now(),
                "error": health.get("error")
            }}
        )
        result = api_doc.copy()
        result.update(health)
        result["last_checked"] = datetime.now()
        logging.info(f"API '{name}' health check: {health['status']}")
        return result
    except Exception as e:
        logging.error(f"check_and_update_api error: {e}")
        return None

def ensure_apis_table():
    """Create APIs collection in MongoDB"""
    try:
        col = get_collection("apis")
        col.create_index([("name", 1)], unique=True)
        print("[DB] APIs collection ready.")
    except Exception as e:
        print(f"[DB] Error ensuring APIs collection: {e}")

