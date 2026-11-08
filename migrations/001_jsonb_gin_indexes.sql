-- Индексы под containment-поиск по JSONB (`@>`). CONCURRENTLY: без блокировки записи в рабочей БД.
-- Выполнять вне транзакции: psql -f migrations/001_jsonb_gin_indexes.sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_abstract_customer_amo_customers_gin
    ON abstract_customer USING gin (amo_customers jsonb_path_ops);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_abstract_customer_amo_leads_gin
    ON abstract_customer USING gin (amo_leads jsonb_path_ops);
