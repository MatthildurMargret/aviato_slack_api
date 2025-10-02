import requests
import os
import logging
import json
import dotenv
import time
from threading import Lock

dotenv.load_dotenv()

aviato_api = os.getenv("AVIATO_API_KEY")
logger = logging.getLogger(__name__)

_last_call = 0.0
_lock = Lock()
_min_delay = 1.5  # seconds

def _rate_limit():
    global _last_call
    with _lock:
        now = time.time()
        elapsed = now - _last_call
        if elapsed < _min_delay:
            time.sleep(_min_delay - elapsed)
        _last_call = time.time()

def get_contact_info(person_id: str):
    """Fetch contact info for a person by ID. Returns dict or None on error."""
    if not person_id:
        return None

    _rate_limit()
    try:
        response = requests.get(
            f"https://data.api.aviato.co/person/{person_id}/contact-info",
            headers={"Authorization": f"Bearer {aviato_api}"},
            timeout=20,
        )
        if response.status_code != 200:
            logger.warning("get_contact_info status %s for person %s", response.status_code, person_id)
            return None
        if not response.text.strip():
            return None
        return response.json()
    except Exception as e:
        logger.warning("get_contact_info error for %s: %s", person_id, e)
        return None