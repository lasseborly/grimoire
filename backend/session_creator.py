"""Background thread that auto-creates session note stubs the day before a scheduled session.

Runs once at startup (catching anything missed while the server was down) then
sleeps until the next UTC midnight and repeats daily.
"""

import threading
import datetime
from typing import Optional

from . import config
from .config import logger
from .db_lock import postgres_advisory_lock

_thread: Optional[threading.Thread] = None
_stop_event = threading.Event()
_SESSION_CREATOR_LOCK = 7384619201345


def _create_upcoming_sessions(db) -> None:
    from .models import Campaign, CampaignSchedule, SessionNote
    from .routers.campaigns._helpers import compute_next_sessions

    tomorrow = (datetime.date.today() + datetime.timedelta(days=1)).isoformat()

    schedules = db.query(CampaignSchedule).all()
    for sched in schedules:
        campaign = db.query(Campaign).filter_by(id=sched.campaign_id).first()
        if not campaign or not campaign.is_gm_campaign:
            continue

        next_dates = compute_next_sessions(sched.definition, n=30)
        if tomorrow not in next_dates:
            continue

        exists = (
            db.query(SessionNote)
            .filter_by(campaign_id=sched.campaign_id, session_date=tomorrow)
            .first()
        )
        if not exists:
            note = SessionNote(
                campaign_id=sched.campaign_id,
                session_date=tomorrow,
                title="",
            )
            db.add(note)
            logger.info(f"Auto-created session note for campaign {sched.campaign_id} on {tomorrow}")

    db.commit()


def _create_once() -> None:
    from .config import SessionLocal

    with postgres_advisory_lock(config.init_database(), _SESSION_CREATOR_LOCK) as acquired:
        if not acquired:
            logger.debug("Session auto-creator is running on another replica.")
            return
        db = SessionLocal()
        try:
            _create_upcoming_sessions(db)
        finally:
            db.close()


def _seconds_until_utc_midnight() -> float:
    now = datetime.datetime.utcnow()
    tomorrow = (now + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(1.0, (tomorrow - now).total_seconds())


def _run() -> None:
    # Run once immediately on startup to catch anything missed
    try:
        _create_once()
    except Exception as e:
        logger.error(f"Session auto-creator startup error: {e}")

    while not _stop_event.is_set():
        secs = _seconds_until_utc_midnight()
        if _stop_event.wait(secs):
            break

        try:
            _create_once()
        except Exception as e:
            logger.error(f"Session auto-creator error: {e}")


def start() -> None:
    global _thread
    _stop_event.clear()
    _thread = threading.Thread(target=_run, daemon=True, name="session-creator")
    _thread.start()


def stop() -> None:
    global _thread
    _stop_event.set()
    if _thread and _thread.is_alive():
        _thread.join(timeout=5)
    _thread = None
    _stop_event.clear()
