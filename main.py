import logging

from models import db
from sync import SyncStats, default_customers_source, default_leads_source, run_sync

logger = logging.getLogger(__name__)


def collect_leads_customers(is_full=True) -> SyncStats:
    """
    Ночная синхронизация покупателей и сделок amoCRM в abstract_customer.

    Бизнес-правила и устройство прогона описаны в sync.py и README.md.
    Вызывается внутри Flask app context (Celery-задача), использует одно соединение и одну транзакцию.
    """
    return run_sync(
        connection_factory=db.engine.connect,
        customers_source=default_customers_source,
        leads_source=default_leads_source,
        is_full=is_full,
    )
