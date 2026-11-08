"""Нагрузочный прогон на фейковой CRM: python bench.py --customers 20000 --leads 50000

Нужен PostgreSQL в TEST_DATABASE_URL (таблица пересоздаётся). Печатает время, число SQL-команд
и пиковую память Python (tracemalloc) для первого прогона (пустая БД) и повторного (все записи уже есть).
"""

import argparse
import os
import resource
import time
import tracemalloc

from flask import Flask
from sqlalchemy import text

from models import db
from sync import run_sync
from tests.conftest import QueryCounter
from tests.fakes import FakeCustomer, FakeLead

BLOB = "x" * 1500  # ~1.5 КБ api_value на сущность


def customers(n):
    for i in range(1, n + 1):
        yield FakeCustomer(i, f"user{i}", name=f"Покупатель {i}", contacts=[i], companies=[i], payload=BLOB)


def leads(n, n_customers):
    for i in range(1, n + 1):
        login = f"user{(i * 7) % (n_customers + n_customers // 10)}" if i % 10 else None  # 10% без логина
        yield FakeLead(1_000_000 + i, login, name=f"Сделка {i}", contacts=[i % n_customers], payload=BLOB)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--customers", type=int, default=20_000)
    p.add_argument("--leads", type=int, default=50_000)
    args = p.parse_args()
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = os.environ["TEST_DATABASE_URL"]
    db.init_app(app)
    with app.app_context():
        db.drop_all()
        db.create_all()
        for label in ("первый прогон (пустая БД)", "повторный прогон (все записи есть)"):
            tracemalloc.start()
            t0 = time.perf_counter()
            with QueryCounter(db.engine) as qc:
                stats = run_sync(db.engine.connect, lambda: customers(args.customers),
                                 lambda: leads(args.leads, args.customers), is_full=True)
            elapsed = time.perf_counter() - t0
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            with db.engine.connect() as conn:
                rows, size = conn.execute(text(
                    "SELECT count(*), pg_size_pretty(pg_total_relation_size('abstract_customer')) FROM abstract_customer"
                )).one()
            print(f"{label}: {elapsed:.1f} с, SQL-команд {qc.count}, пик памяти Python {peak / 2**20:.0f} МБ, "
                  f"записей {rows} ({size}); {stats}")
    print(f"max RSS процесса: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024:.0f} МБ")


if __name__ == "__main__":
    main()
