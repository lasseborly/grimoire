"""Database-backed locks for work shared by PostgreSQL-backed replicas."""

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import text
from sqlalchemy.engine import Engine


@contextmanager
def postgres_advisory_lock(engine: Engine, key: int) -> Iterator[bool]:
    """Try a session-level lock; non-PostgreSQL databases need no extra lock."""
    if engine.dialect.name != "postgresql":
        yield True
        return

    with engine.connect() as connection:
        connection = connection.execution_options(isolation_level="AUTOCOMMIT")
        acquired = bool(
            connection.scalar(
                text("SELECT pg_try_advisory_lock(:key)"),
                {"key": key},
            )
        )
        try:
            yield acquired
        finally:
            if acquired:
                try:
                    connection.execute(
                        text("SELECT pg_advisory_unlock(:key)"),
                        {"key": key},
                    )
                except Exception:
                    connection.invalidate()
                    raise
