"""Бизнес-правила ночного прогона из README задания."""

import pytest

from models import db
from tests.conftest import QueryCounter, add_record, ids, records, sync
from tests.fakes import BrokenLead, FakeCustomer, FakeLead


# ------------------------------------------------------------------ покупатели

def test_customer_without_login_is_skipped(app):
    stats = sync([FakeCustomer(1, None), FakeCustomer(2, "b")])
    assert stats.customers == 1 and stats.customers_skipped == 1
    [rec] = records().values()
    assert ids(rec["amo_customers"]) == [2]


def test_customer_found_by_crm_id_is_updated_in_place(app):
    rec_id = add_record(customers=[(1, "a", "old")])
    sync([FakeCustomer(1, "a", name="new")])
    recs = records()
    assert list(recs) == [rec_id]
    [c] = recs[rec_id]["amo_customers"]
    assert c["general"]["name"] == "new" and c["general"]["login"] == "a"


def test_customer_found_by_login_among_saved_leads(app):
    rec_id = add_record(leads=[(10, "x")])
    sync([FakeCustomer(5, "x")], [FakeLead(10, "x")])
    recs = records()
    assert list(recs) == [rec_id]
    assert ids(recs[rec_id]["amo_customers"]) == [5] and ids(recs[rec_id]["amo_leads"]) == [10]


def test_customer_ambiguous_login_match_is_not_used(app):
    first = add_record(leads=[(10, "x")])
    second = add_record(leads=[(11, "x")])
    sync([FakeCustomer(5, "x")], [], is_full=False)
    recs = records()
    new = [r for r in recs if r not in (first, second)]
    assert len(new) == 1 and ids(recs[new[0]]["amo_customers"]) == [5]
    assert not recs[first]["amo_customers"] and not recs[second]["amo_customers"]


# ------------------------------------------------------------------ сделки

def test_lead_found_by_crm_id(app):
    rec_id = add_record(customers=[(1, "a")], leads=[(10, "zzz", "old")])
    sync([FakeCustomer(1, "a")], [FakeLead(10, None, name="new")])
    [lead] = records()[rec_id]["amo_leads"]
    assert lead["general"]["name"] == "new"


def test_lead_found_by_login_among_customers(app):
    rec_id = add_record(customers=[(1, "a")])
    sync([FakeCustomer(1, "a")], [FakeLead(10, "a")])
    recs = records()
    assert list(recs) == [rec_id] and ids(recs[rec_id]["amo_leads"]) == [10]


def test_lead_merges_two_records_into_the_one_found_by_lead(app):
    by_lead = add_record(leads=[(10, "m")], send_methods=[{"type": "mail", "value": "a@x", "desc": ""}])
    by_login = add_record(customers=[(5, "m")], contacts=[(7,)],
                          send_methods=[{"type": "sms", "value": "+7", "desc": ""}])
    stats = sync([FakeCustomer(5, "m", contacts=[7])], [FakeLead(10, "m", name="after merge")])
    recs = records()
    assert list(recs) == [by_lead]  # вторая запись удалена
    rec = recs[by_lead]
    assert ids(rec["amo_customers"]) == [5] and ids(rec["amo_contacts"]) == [7]
    assert rec["amo_leads"][0]["general"]["name"] == "after merge"  # сама сделка тоже записана
    assert {m["type"] for m in rec["send_methods"]} == {"mail", "sms"}
    assert stats.merges == 1


def test_lead_without_any_match_creates_record(app):
    sync([FakeCustomer(1, "a")], [FakeLead(10, None), FakeLead(11, "unknown")])
    recs = records()
    assert len(recs) == 3
    assert sorted(ids(r["amo_leads"]) for r in recs.values()) == [[], [10], [11]]


# ------------------------------------------------------------------ чистка

def test_full_pass_cleanup(app):
    untouched = add_record(customers=[(99, "gone")])
    kept = add_record(customers=[(1, "a"), (2, "b")], leads=[(10, "a"), (11, "a")],
                      contacts=[(7,), (8,)], companies=[(70,)])
    emptied = add_record(customers=[(3, "c")])
    sync([FakeCustomer(1, "a", contacts=[7]), FakeCustomer(3, "c", name="Удалить дубль")],
         [FakeLead(10, "a")])
    recs = records()
    assert untouched not in recs  # покупателя не было в выгрузке
    assert emptied not in recs  # единственный покупатель с «удалить» в имени -> запись пустая
    rec = recs[kept]
    assert ids(rec["amo_customers"]) == [1] and ids(rec["amo_leads"]) == [10]
    assert ids(rec["amo_contacts"]) == [7] and ids(rec["amo_companies"]) == []


def test_lead_contacts_count_as_seen(app):
    rec_id = add_record(customers=[(1, "a")], leads=[(10, "a")], contacts=[(7,), (8,)])
    sync([FakeCustomer(1, "a")], [FakeLead(10, "a", contacts=[8])])
    assert ids(records()[rec_id]["amo_contacts"]) == [8]


def test_interrupted_collection_keeps_updates_and_skips_cleanup(app):
    untouched = add_record(customers=[(99, "gone")])

    def leads():
        yield FakeLead(10, "a")
        raise ConnectionError("CRM недоступна")

    from sync import run_sync
    with pytest.raises(ConnectionError):
        run_sync(db.engine.connect, lambda: iter([FakeCustomer(1, "a")]), leads, is_full=True)
    recs = records()
    assert untouched in recs
    synced = [r for r in recs.values() if ids(r["amo_customers"]) == [1]]
    assert len(synced) == 1 and ids(synced[0]["amo_leads"]) == [10]


def test_partial_pass_skips_statuses_and_never_deletes(app):
    untouched = add_record(customers=[(99, "gone")])
    stats = sync([FakeCustomer(1, "a"), FakeCustomer(2, "b", status_id=111349)],
                 [FakeLead(10, "a"), FakeLead(11, "b", status_id=143)], is_full=False)
    assert stats.customers_skipped == 1 and stats.leads_skipped == 1
    recs = records()
    assert untouched in recs and len(recs) == 2


def test_empty_export_does_not_wipe_table(app):
    rec_id = add_record(customers=[(1, "a")])
    sync([], [])
    assert rec_id in records()


def test_broken_entity_is_protected_from_cleanup(app):
    rec_id = add_record(customers=[(1, "a")], leads=[(10, "a"), (11, "a")])
    stats = sync([FakeCustomer(1, "a")], [FakeLead(10, "a"), BrokenLead(11, "a")])
    assert stats.errors == 1
    assert ids(records()[rec_id]["amo_leads"]) == [10, 11]


def test_rerun_is_idempotent(app):
    data = ([FakeCustomer(i, f"u{i}") for i in range(1, 50)], [FakeLead(100 + i, f"u{i}") for i in range(1, 80)])
    sync(*data)
    first = records()
    stats = sync(*data)
    assert records() == first
    assert stats.created == 0 and stats.updated == 0 and stats.unchanged == len(first)


# ------------------------------------------------------------------ нагрузка

def test_query_count_does_not_grow_per_entity(app):
    customers = [FakeCustomer(i, f"user{i}", contacts=[i]) for i in range(1, 3001)]
    leads = [FakeLead(10_000 + i, f"user{i % 3500}") for i in range(1, 7001)]
    with QueryCounter(db.engine) as first:
        sync(customers, leads)
    with QueryCounter(db.engine) as second:
        sync(customers, leads)
    # 10 тыс. сущностей: пачки по 1000 в staging + блоки по 500 записей; без запроса на сущность
    assert first.count < 60, first.count
    assert second.count < 60, second.count
