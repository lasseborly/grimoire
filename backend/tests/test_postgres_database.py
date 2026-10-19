"""Optional PostgreSQL migration and search smoke test."""
import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from backend.models.db import init_db
from backend.db_lock import postgres_advisory_lock
from backend.routers.content._helpers import search_entry_ids
from backend.routers.search._helpers import full_text_search_parts


@pytest.mark.skipif(not os.environ.get("POSTGRES_TEST_URL"), reason="PostgreSQL URL not configured")
def test_postgres_migrations_and_full_text_search():
    base_url = make_url(os.environ["POSTGRES_TEST_URL"])
    admin_engine = create_engine(base_url)
    schema = f"grimoire_test_{uuid.uuid4().hex}"
    app_engine = None
    with admin_engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))

    test_url = base_url.update_query_dict({"options": f"-csearch_path={schema}"})
    try:
        app_engine, session_factory = init_db(test_url.render_as_string(hide_password=False))
        lock_key = 7384619201346
        with postgres_advisory_lock(app_engine, lock_key) as acquired:
            assert acquired
            with postgres_advisory_lock(app_engine, lock_key) as second_acquired:
                assert not second_acquired
        with postgres_advisory_lock(app_engine, lock_key) as reacquired:
            assert reacquired

        with session_factory() as db:
            db.execute(
                text(
                    "INSERT INTO book_search (book_id, page_number, content) "
                    "VALUES (:book_id, 1, :content)"
                ),
                {"book_id": "test-book", "content": "A wizard explores the ancient tower"},
            )
            search = full_text_search_parts(db, "wizard")
            hit = db.execute(
                text(
                    "SELECT book_id, "
                    f"{search['snippet']} AS snippet, {search['rank']} AS rank "
                    "FROM book_search "
                    f"WHERE {search['match']} ORDER BY {search['order']} LIMIT 1"
                ),
                {"query": search["query"]},
            ).one()
            assert hit.book_id == "test-book"
            assert "\ue000" in hit.snippet and "\ue001" in hit.snippet

            db.execute(
                text(
                    "INSERT INTO content_search "
                    "(entry_row, pack_row, schema_id, content_type, name, body) "
                    "VALUES ('entry-1', 'pack-1', 'test-schema', 'spells', "
                    "'Fire Bolt', 'A spark of fire')"
                )
            )
            assert search_entry_ids(db, "test-schema", "spells", "fire") == ["entry-1"]
    finally:
        if app_engine is not None:
            app_engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin_engine.dispose()
