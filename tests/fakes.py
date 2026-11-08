"""Фейковые сущности CRM: клиента amoCRM в репозитории нет, поэтому прогон тестируется на них."""

from __future__ import annotations

from typing import Iterable, Optional

from amocrm import Customer, Lead


def _fields(login: Optional[str]) -> dict:
    return {"Логин": {"value": login}} if login else {}


class FakeCustomer(Customer):
    def __init__(self, id: int, login: Optional[str], name: str = "Покупатель", status_id: int = 1,
                 contacts: Iterable[int] = (), companies: Iterable[int] = (), payload: str = ""):
        super().__init__(
            id=id, name=name, next_price=None, next_date=None, responsible_user_id=None, status_id=status_id,
            periodicity=None, created_by=None, updated_by=None, created_at=None, updated_at=None,
            closest_task_at=None, is_deleted=False, ltv=None, purchases_count=None, average_check=None,
            account_id=None, custom_fields_values=_fields(login),
            _embedded={"contacts": [{"id": c} for c in contacts], "companies": [{"id": c} for c in companies]},
        )
        self._payload = payload

    def as_dict(self) -> dict:
        return {"id": self.id, "name": self.name, "status_id": self.status_id, "blob": self._payload}


class FakeLead(Lead):
    def __init__(self, id: int, login: Optional[str], name: str = "Сделка", status_id: int = 1,
                 contacts: Iterable[int] = (), payload: str = ""):
        super().__init__(id=id, name=name, status_id=status_id, custom_fields_values=_fields(login),
                         _embedded={"contacts": [{"id": c} for c in contacts]})
        self._payload = payload

    def as_dict(self) -> dict:
        return {"id": self.id, "name": self.name, "status_id": self.status_id, "blob": self._payload}


class BrokenLead(FakeLead):
    """Сделка с повреждёнными данными: обработка падает на чтении полей."""

    def get_field(self, field_name):
        raise ValueError("broken custom_fields_values")
