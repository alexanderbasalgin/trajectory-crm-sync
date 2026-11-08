import json
import os
import sys
from pathlib import Path

import pytest
from flask import Flask
from sqlalchemy import event, text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models import AbstractCustomer, db  # noqa: E402
from sync import run_sync  # noqa: E402


@pytest.fixture(scope="session")
def app():
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL не задан (нужен PostgreSQL)")
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = url
    db.init_app(app)
    with app.app_context():
        db.drop_all()
        db.create_all()
        yield app
        db.session.remove()


@pytest.fixture(autouse=True)
def clean(app):
    with db.engine.begin() as conn:
        conn.execute(text("TRUNCATE abstract_customer RESTART IDENTITY"))
    yield


def sync(customers=(), leads=(), is_full=True):
    return run_sync(db.engine.connect, lambda: iter(list(customers)), lambda: iter(list(leads)), is_full=is_full)


def add_record(customers=(), leads=(), contacts=(), companies=(), send_methods=None) -> int:
    def entity(eid, login=None, name="x"):
        return {"general": {"id": eid, "login": login, "name": name}, "api_value": {}}
    with db.engine.begin() as conn:
        return conn.execute(
            text("""INSERT INTO abstract_customer (amo_customers, amo_leads, amo_contacts, amo_companies, send_methods)
                    VALUES (CAST(:c AS jsonb), CAST(:l AS jsonb), CAST(:ct AS jsonb), CAST(:co AS jsonb),
                            CAST(:sm AS jsonb)) RETURNING id"""),
            {"c": json.dumps([entity(*c) for c in customers]), "l": json.dumps([entity(*l) for l in leads]),
             "ct": json.dumps([entity(*c) for c in contacts]), "co": json.dumps([entity(*c) for c in companies]),
             "sm": json.dumps(send_methods or [])},
        ).scalar_one()


def records() -> dict[int, dict]:
    with db.engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT id, amo_customers, amo_leads, amo_contacts, amo_companies, send_methods FROM abstract_customer"
        )).mappings()
        return {r["id"]: dict(r) for r in rows}


def ids(items) -> list:
    return sorted(i["general"]["id"] for i in items or [])


class QueryCounter:
    def __init__(self, engine):
        self.engine, self.count = engine, 0

    def _on(self, *args, **kwargs):
        self.count += 1

    def __enter__(self):
        event.listen(self.engine, "before_cursor_execute", self._on)
        return self

    def __exit__(self, *exc):
        event.remove(self.engine, "before_cursor_execute", self._on)
