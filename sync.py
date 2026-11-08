"""Ночная синхронизация покупателей и сделок amoCRM -> abstract_customer.

Схема работы (вместо запроса и commit на каждую сущность):

1. Индекс. Одним потоковым запросом читаем из Postgres только идентификаторы и логины вложенных
   покупателей и сделок (без api_value). По нему в памяти за O(1) выполняются все поиски из бизнес-правил.
2. Сбор. Сущности из CRM обрабатываются по одной в исходном порядке: решения о поиске, создании и слиянии
   принимаются по индексу, а JSON сущностей пачками по STAGE_BATCH строк пишется во временную таблицу.
   Память процесса не зависит от объёма выгрузки: в ней только индекс и текущая пачка.
3. Применение. Затронутые записи обрабатываются блоками по APPLY_CHUNK: один SELECT строк блока,
   один SELECT их сущностей из staging, слияния и фильтрация в Python, один UPDATE и один INSERT на блок
   через jsonb_to_recordset. Всё в одной транзакции и с одним commit в конце.
4. Чистка (только полный проход, только если сбор не прервался) выполняется в том же проходе,
   без повторного чтения всей таблицы.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import count
from typing import Any, Callable, Iterable, Optional

from sqlalchemy import BigInteger, Column, Connection, Integer, MetaData, String, Table, text
from sqlalchemy.dialects.postgresql import JSONB

from abstract_customer import get_customer_json, get_lead_json
from amocrm import COMPANIES, CONTACTS, Customer, Lead

logger = logging.getLogger(__name__)

STAGE_BATCH = 1000
APPLY_CHUNK = 500
INDEX_FETCH = 5000
SKIP_CUSTOMER_STATUS = 111349
SKIP_LEAD_STATUS = 143
DELETE_MARKER = "удалить"
ENTITY_ATTRS = ("amo_customers", "amo_leads", "amo_contacts", "amo_companies")
ALL_ATTRS = ENTITY_ATTRS + ("send_methods",)

_stage_meta = MetaData()
stage_table = Table(
    "sync_stage", _stage_meta,
    Column("seq", Integer, nullable=False),
    Column("kind", String(8), nullable=False),
    Column("crm_id", BigInteger, nullable=False),
    Column("rec_key", BigInteger, nullable=False),
    Column("payload", JSONB, nullable=False),
    prefixes=["TEMPORARY"],
    postgresql_on_commit="DROP",
)

STAGE_INSERT_SQL = text("""
    INSERT INTO sync_stage (seq, kind, crm_id, rec_key, payload)
    SELECT seq, kind, crm_id, rec_key, payload
    FROM jsonb_to_recordset(CAST(:rows AS jsonb)) AS v(seq integer, kind text, crm_id bigint, rec_key bigint, payload jsonb)
""")

INDEX_SQL = text("""
    SELECT a.id,
           coalesce((SELECT jsonb_agg(jsonb_build_array(e -> 'general' -> 'id', e -> 'general' ->> 'login'))
                     FROM jsonb_array_elements(CASE WHEN jsonb_typeof(a.amo_customers) = 'array'
                                                    THEN a.amo_customers ELSE '[]'::jsonb END) e), '[]') AS customers,
           coalesce((SELECT jsonb_agg(jsonb_build_array(e -> 'general' -> 'id', e -> 'general' ->> 'login'))
                     FROM jsonb_array_elements(CASE WHEN jsonb_typeof(a.amo_leads) = 'array'
                                                    THEN a.amo_leads ELSE '[]'::jsonb END) e), '[]') AS leads
    FROM abstract_customer a
""")


# --------------------------------------------------------------------------- индекс

class RecordIndex:
    """Где лежит каждая сущность и какие записи содержат логин. Только id и логины, без JSON."""

    def __init__(self) -> None:
        self._temp_keys = count(-1, -1)  # отрицательные ключи = записи, которых ещё нет в БД
        self.customer_rec: dict[int, int] = {}
        self.lead_rec: dict[int, int] = {}
        self._customer_login: dict[int, Optional[str]] = {}
        self._lead_login: dict[int, Optional[str]] = {}
        self._recs_by_customer_login: dict[str, dict[int, int]] = defaultdict(dict)
        self._recs_by_lead_login: dict[str, dict[int, int]] = defaultdict(dict)
        self.rec_customers: dict[int, set[int]] = defaultdict(set)
        self.rec_leads: dict[int, set[int]] = defaultdict(set)

    @staticmethod
    def _bump(index: dict[str, dict[int, int]], login: Optional[str], rec: int, delta: int) -> None:
        if not login:
            return
        recs = index[login]
        recs[rec] = recs.get(rec, 0) + delta
        if recs[rec] <= 0:
            del recs[rec]
        if not recs:
            del index[login]

    def new_record(self) -> int:
        return next(self._temp_keys)

    def put_customer(self, rec: int, customer_id: int, login: Optional[str]) -> None:
        if customer_id in self.customer_rec:
            old_rec = self.customer_rec[customer_id]
            self._bump(self._recs_by_customer_login, self._customer_login.get(customer_id), old_rec, -1)
            self.rec_customers[old_rec].discard(customer_id)
        self.customer_rec[customer_id] = rec
        self._customer_login[customer_id] = login
        self.rec_customers[rec].add(customer_id)
        self._bump(self._recs_by_customer_login, login, rec, +1)

    def put_lead(self, rec: int, lead_id: int, login: Optional[str]) -> None:
        if lead_id in self.lead_rec:
            old_rec = self.lead_rec[lead_id]
            self._bump(self._recs_by_lead_login, self._lead_login.get(lead_id), old_rec, -1)
            self.rec_leads[old_rec].discard(lead_id)
        self.lead_rec[lead_id] = rec
        self._lead_login[lead_id] = login
        self.rec_leads[rec].add(lead_id)
        self._bump(self._recs_by_lead_login, login, rec, +1)

    @staticmethod
    def _unique(index: dict[str, dict[int, int]], login: str) -> Optional[int]:
        recs = index.get(login)
        if not recs or len(recs) != 1:
            return None  # больше одной записи с логином: совпадение не используется
        return next(iter(recs))

    def unique_rec_by_lead_login(self, login: str) -> Optional[int]:
        return self._unique(self._recs_by_lead_login, login)

    def unique_rec_by_customer_login(self, login: str) -> Optional[int]:
        return self._unique(self._recs_by_customer_login, login)

    def merge(self, src: int, dst: int) -> None:
        for customer_id in list(self.rec_customers.pop(src, ())):
            self.put_customer(dst, customer_id, self._customer_login.get(customer_id))
        for lead_id in list(self.rec_leads.pop(src, ())):
            self.put_lead(dst, lead_id, self._lead_login.get(lead_id))

    @classmethod
    def load(cls, conn: Connection) -> "RecordIndex":
        index = cls()
        result = conn.execute(INDEX_SQL, execution_options={"stream_results": True, "yield_per": INDEX_FETCH})
        for rec_id, customers, leads in result:
            for entity_id, login in customers:
                if entity_id is not None:
                    index.put_customer(rec_id, int(entity_id), login)
            for entity_id, login in leads:
                if entity_id is not None:
                    index.put_lead(rec_id, int(entity_id), login)
        return index


# --------------------------------------------------------------------------- состояние прогона

@dataclass
class SyncStats:
    customers: int = 0
    customers_skipped: int = 0
    leads: int = 0
    leads_skipped: int = 0
    errors: int = 0
    merges: int = 0
    created: int = 0
    updated: int = 0
    unchanged: int = 0
    deleted_untouched: int = 0
    deleted_merged: int = 0
    deleted_empty: int = 0
    interrupted: bool = False


@dataclass
class SyncState:
    index: RecordIndex
    full: bool
    stats: SyncStats = field(default_factory=SyncStats)
    # что пришло в этой выгрузке по каждой записи (аналог ids_map исходного кода)
    seen: dict[int, dict[str, set[int]]] = field(
        default_factory=lambda: defaultdict(lambda: {a: set() for a in ENTITY_ATTRS}))
    merged_into: dict[int, int] = field(default_factory=dict)
    merge_log: list[tuple[int, int]] = field(default_factory=list)
    staged_keys: set[int] = field(default_factory=set)
    _buffer: list[dict[str, Any]] = field(default_factory=list)
    _seq: Any = field(default_factory=lambda: count(1))

    def final(self, rec: int) -> int:
        while rec in self.merged_into:
            rec = self.merged_into[rec]
        return rec

    def stage(self, kind: str, crm_id: int, rec: int, payload: dict) -> None:
        self._buffer.append({"seq": next(self._seq), "kind": kind, "crm_id": crm_id, "rec_key": rec, "payload": payload})
        self.staged_keys.add(rec)

    def flush(self, conn: Connection) -> None:
        # Одна команда INSERT на пачку. executemany здесь не подходит: psycopg выполняет его
        # построчно (пусть и в pipeline), то есть отдельной командой на каждую сущность.
        if self._buffer:
            conn.execute(STAGE_INSERT_SQL, {"rows": json.dumps(self._buffer, ensure_ascii=False)})
            self._buffer.clear()

    def merge(self, src: int, dst: int) -> None:
        self.index.merge(src, dst)
        self.merged_into[src] = dst
        self.merge_log.append((src, dst))
        if src in self.seen:
            src_seen = self.seen.pop(src)
            for attr, ids in src_seen.items():
                self.seen[dst][attr] |= ids
        self.stats.merges += 1


# --------------------------------------------------------------------------- бизнес-правила

def _ids(items: Iterable[dict]) -> set[int]:
    return {item["id"] for item in items or () if item.get("id") is not None}


def process_customer(state: SyncState, customer: Customer) -> None:
    if not state.full and customer.status_id == SKIP_CUSTOMER_STATUS:
        state.stats.customers_skipped += 1
        return
    login = customer.get_field("Логин")
    if not login:
        state.stats.customers_skipped += 1  # покупатель без логина пропускается
        return
    payload = get_customer_json(customer)  # до изменения индекса: ошибка данных не оставит его в полусостоянии
    contacts, companies = _ids(customer.contacts), _ids(customer.companies)
    index = state.index
    rec = index.customer_rec.get(customer.id)  # по id покупателя в CRM
    if rec is None:
        rec = index.unique_rec_by_lead_login(login)  # иначе по логину среди сохранённых сделок
    if rec is None:
        rec = index.new_record()
    index.put_customer(rec, customer.id, login)
    state.stage("customer", customer.id, rec, payload)
    seen = state.seen[rec]
    seen["amo_customers"].add(customer.id)
    seen["amo_contacts"] |= contacts
    seen["amo_companies"] |= companies
    state.stats.customers += 1


def process_lead(state: SyncState, lead: Lead) -> None:
    if not state.full and lead.status_id == SKIP_LEAD_STATUS:
        state.stats.leads_skipped += 1
        return
    login = lead.get_field("Логин")
    payload = get_lead_json(lead)
    contacts = _ids(lead.contacts)
    index = state.index
    rec_by_lead = index.lead_rec.get(lead.id)  # по id сделки в CRM
    rec_by_login = index.unique_rec_by_customer_login(login) if login else None  # по логину среди покупателей
    if rec_by_lead is not None and rec_by_login is not None and rec_by_lead != rec_by_login:
        state.merge(src=rec_by_login, dst=rec_by_lead)  # сливаем в найденную по сделке, вторую удаляем
        rec = rec_by_lead
    else:
        rec = rec_by_lead if rec_by_lead is not None else rec_by_login
    if rec is None:
        rec = index.new_record()  # не нашлось ничего: новая запись
    index.put_lead(rec, lead.id, login)
    state.stage("lead", lead.id, rec, payload)
    seen = state.seen[rec]
    seen["amo_leads"].add(lead.id)
    seen["amo_contacts"] |= contacts
    state.stats.leads += 1


def _protect(state: SyncState, kind: str, entity: Any) -> None:
    """Сущность не удалось обработать: не даём чистке удалить её прежнюю версию."""
    entity_id = getattr(entity, "id", None)
    rec = (state.index.customer_rec if kind == "customer" else state.index.lead_rec).get(entity_id)
    if rec is not None:
        state.seen[rec]["amo_customers" if kind == "customer" else "amo_leads"].add(entity_id)


def _collect(conn: Connection, state: SyncState, entities: Iterable, kind: str,
             handler: Callable[[SyncState, Any], None]) -> None:
    for entity in entities:
        try:
            handler(state, entity)  # только память: в БД здесь ничего не пишется
        except Exception:
            state.stats.errors += 1
            _protect(state, kind, entity)
            logger.exception("Не удалось обработать %s %s", kind, entity)
        if len(state._buffer) >= STAGE_BATCH:
            state.flush(conn)  # ошибка записи в БД - это прерывание прогона, а не ошибка одной сущности


# --------------------------------------------------------------------------- применение

def _key(item: dict) -> Any:
    general = item.get("general") if isinstance(item, dict) else None
    entity_id = general.get("id") if isinstance(general, dict) else None
    return ("id", entity_id) if entity_id is not None else ("obj", json.dumps(item, sort_keys=True, ensure_ascii=False))


def merge_entities(base: list, other: list) -> list:
    """Объединение по general.id; при совпадении поля из other перекрывают base (как dict_compare_merge)."""
    merged: dict[Any, dict] = {}
    for item in list(base or []) + list(other or []):
        key = _key(item)
        if key in merged and isinstance(item, dict):
            merged[key] = {**merged[key], **item}
        else:
            merged[key] = item
    return list(merged.values())


def merge_send_methods(base: list, other: list) -> list:
    out, keys = [], set()
    for item in list(base or []) + list(other or []):
        key = (item.get("type"), item.get("value")) if isinstance(item, dict) else json.dumps(item, sort_keys=True)
        if key not in keys:
            keys.add(key)
            out.append(item)
    return out


def upsert_entity(items: list, payload: dict) -> list:
    entity_id = payload["general"]["id"]
    out = [item for item in (items or []) if _key(item) != ("id", entity_id)]
    position = next((i for i, item in enumerate(items or []) if _key(item) == ("id", entity_id)), None)
    if position is None:
        out.append(payload)
    else:
        out.insert(position, payload)
    return out


def _is_marked_for_delete(item: dict) -> bool:
    name = ((item or {}).get("general") or {}).get("name") or ""
    return DELETE_MARKER in str(name).lower()


def filter_entities(items: list, allowed_ids: set[int]) -> list:
    """Чистка полного прохода: остаются сущности из этой выгрузки без «удалить» в имени."""
    return [item for item in items or []
            if _key(item)[0] == "id" and _key(item)[1] in allowed_ids and not _is_marked_for_delete(item)]


def _empty_row() -> dict[str, list]:
    return {attr: [] for attr in ALL_ATTRS}


def apply(conn: Connection, state: SyncState, cleanup: bool) -> None:
    state.flush(conn)
    stats = state.stats
    members: dict[int, list[int]] = defaultdict(list)
    for rec in set(state.seen) | state.staged_keys | set(state.merged_into):
        members[state.final(rec)].append(rec)
    finals = sorted(members)
    existing_members = sorted(r for recs in members.values() for r in recs if r > 0)

    if cleanup:
        # покупатели, которых не было в выгрузке: одна команда вместо чтения всей таблицы
        stats.deleted_untouched = conn.execute(
            text("DELETE FROM abstract_customer WHERE NOT (id = ANY(:keep))"), {"keep": existing_members}
        ).rowcount

    to_delete: list[int] = [r for r in state.merged_into if r > 0]
    merge_events = defaultdict(list)
    for src, dst in state.merge_log:
        merge_events[state.final(dst)].append((src, dst))

    for start in range(0, len(finals), APPLY_CHUNK):
        chunk = finals[start:start + APPLY_CHUNK]
        chunk_members = [r for f in chunk for r in members[f]]
        rows = {r: _empty_row() for r in chunk_members}
        db_rows = conn.execute(
            text(f"SELECT id, {', '.join(ALL_ATTRS)} FROM abstract_customer WHERE id = ANY(:ids)"),
            {"ids": [r for r in chunk_members if r > 0]},
        ).mappings()
        original = {}
        for row in db_rows:
            rows[row["id"]] = {attr: row[attr] or [] for attr in ALL_ATTRS}
            original[row["id"]] = {attr: row[attr] for attr in ALL_ATTRS}

        for final in chunk:  # слияния в хронологическом порядке
            for src, dst in merge_events.get(final, ()):
                target, source = rows.setdefault(dst, _empty_row()), rows.pop(src, _empty_row())
                for attr in ENTITY_ATTRS:
                    target[attr] = merge_entities(target[attr], source[attr])
                target["send_methods"] = merge_send_methods(target["send_methods"], source["send_methods"])

        staged = conn.execute(
            text("SELECT kind, rec_key, payload FROM sync_stage WHERE rec_key = ANY(:keys) ORDER BY seq"),
            {"keys": chunk_members},
        )
        for kind, rec_key, payload in staged:
            target = rows[state.final(rec_key)]
            attr = "amo_customers" if kind == "customer" else "amo_leads"
            target[attr] = upsert_entity(target[attr], payload)

        updates, inserts, empties = [], [], []
        for final in chunk:
            row = rows[final]
            if cleanup:
                seen = state.seen.get(final, {a: set() for a in ENTITY_ATTRS})
                for attr in ENTITY_ATTRS:
                    row[attr] = filter_entities(row[attr], seen[attr])
                if not row["amo_customers"] and not row["amo_leads"]:
                    if final > 0:
                        empties.append(final)
                    continue
            if final < 0:
                inserts.append(row)
            elif {a: original.get(final, {}).get(a) or [] for a in ALL_ATTRS} != row:
                updates.append({"id": final, **row})
            else:
                stats.unchanged += 1

        if updates:
            conn.execute(text(f"""
                UPDATE abstract_customer a
                SET {', '.join(f'{c} = v.{c}' for c in ALL_ATTRS)}, updated_at = now()
                FROM jsonb_to_recordset(CAST(:rows AS jsonb))
                     AS v(id bigint, {', '.join(f'{c} jsonb' for c in ALL_ATTRS)})
                WHERE a.id = v.id
            """), {"rows": json.dumps(updates, ensure_ascii=False)})
            stats.updated += len(updates)
        if inserts:
            stats.created += len(inserts)
            conn.execute(text(f"""
                INSERT INTO abstract_customer ({', '.join(ALL_ATTRS)})
                SELECT {', '.join(ALL_ATTRS)}
                FROM jsonb_to_recordset(CAST(:rows AS jsonb)) AS v({', '.join(f'{c} jsonb' for c in ALL_ATTRS)})
            """), {"rows": json.dumps(inserts, ensure_ascii=False)})
        to_delete.extend(empties)
        stats.deleted_empty += len(empties)

    if to_delete:
        conn.execute(text("DELETE FROM abstract_customer WHERE id = ANY(:ids)"), {"ids": sorted(set(to_delete))})
    stats.deleted_merged = len([r for r in state.merged_into if r > 0])


# --------------------------------------------------------------------------- точка входа

def run_sync(connection_factory: Callable[[], Any], customers_source: Callable[[], Iterable[Customer]],
             leads_source: Callable[[], Iterable[Lead]], is_full: bool = True) -> SyncStats:
    """Один прогон в одной транзакции. Если сбор из CRM прервался, собранное сохраняется, а чистка не запускается."""
    with connection_factory() as conn:
        with conn.begin():
            state = SyncState(index=RecordIndex.load(conn), full=is_full)
            stage_table.create(conn)
            error: Optional[BaseException] = None
            try:
                _collect(conn, state, customers_source(), "customer", process_customer)
                logger.info("Collected customers is_full=%s stats=%s", is_full, state.stats)
                _collect(conn, state, leads_source(), "lead", process_lead)
                logger.info("Collected leads is_full=%s stats=%s", is_full, state.stats)
            except Exception as exc:  # сбор прервался: обновления сохраняем, удаление не запускаем
                error = exc
                state.stats.interrupted = True
                logger.exception("Сбор из CRM прервался, чистка не выполняется")
            cleanup = is_full and error is None
            if cleanup and state.stats.customers == 0:
                # пустая выгрузка (например, CRM ответила 204 на первой странице) не должна стереть таблицу
                logger.error("В выгрузке нет ни одного покупателя: чистка пропущена")
                cleanup = False
            apply(conn, state, cleanup=cleanup)
        logger.info("Sync finished stats=%s", state.stats)
    if error is not None:
        raise error
    return state.stats


def default_customers_source() -> Iterable[Customer]:
    from amocrm import amocrm_get
    return amocrm_get(Customer, _with=f"{COMPANIES},{CONTACTS}")


def default_leads_source() -> Iterable[Lead]:
    from amocrm import amocrm_get
    return amocrm_get(Lead, _with=f"{CONTACTS}")
