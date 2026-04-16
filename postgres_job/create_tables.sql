CREATE TABLE IF NOT EXISTS earthquake_enriched (
    event_id       TEXT PRIMARY KEY,
    magnitude      DOUBLE PRECISION,
    mag_type       TEXT,
    place          TEXT,
    event_ts       TIMESTAMPTZ,
    latitude       DOUBLE PRECISION,
    longitude      DOUBLE PRECISION,
    depth_km       DOUBLE PRECISION,
    significance   INTEGER,
    tsunami        INTEGER,
    status         TEXT,
    type           TEXT,
    title          TEXT,
    depth_category TEXT,
    severity       TEXT,
    is_significant INTEGER,
    processed_at   TIMESTAMPTZ
);

-- Staging table: ecrite en "overwrite + truncate" par chaque
-- micro-batch Spark, puis INSERT ... ON CONFLICT DO NOTHING
-- vers la table principale. Permet a Postgres de rester
-- idempotent en cas de rejeu d'un batch (at-least-once).
-- Pas de PK ici: c'est un buffer.
CREATE TABLE IF NOT EXISTS earthquake_enriched_stg (
    event_id       TEXT,
    magnitude      DOUBLE PRECISION,
    mag_type       TEXT,
    place          TEXT,
    event_ts       TIMESTAMPTZ,
    latitude       DOUBLE PRECISION,
    longitude      DOUBLE PRECISION,
    depth_km       DOUBLE PRECISION,
    significance   INTEGER,
    tsunami        INTEGER,
    status         TEXT,
    type           TEXT,
    title          TEXT,
    depth_category TEXT,
    severity       TEXT,
    is_significant INTEGER,
    processed_at   TIMESTAMPTZ
);
