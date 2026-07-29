-- V3-1c: enforce version pinning (BM-114).
--
-- Runs have recorded journey_version since V3-1a, but only the LATEST
-- definition was stored — so the scheduler had to execute whatever the journey
-- currently said and could only warn on a mismatch. Pinning was recorded, not
-- enforced.
--
-- Definitions are immutable per version, which makes this table append-only and
-- trivially cacheable: a (journey, version) pair never changes meaning.
CREATE TABLE IF NOT EXISTS journey_versions (
    workspace_id VARCHAR(64)  NOT NULL,
    journey_id   VARCHAR(64)  NOT NULL,
    version      INT UNSIGNED NOT NULL,
    trigger_def  JSON         NOT NULL,
    steps        JSON         NOT NULL,
    created_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (workspace_id, journey_id, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Backfill v1 for journeys created before this migration, so existing runs can
-- resolve the definition they pinned.
INSERT IGNORE INTO journey_versions (workspace_id, journey_id, version, trigger_def, steps, created_at)
SELECT workspace_id, journey_id, version, trigger_def, steps, created_at FROM journeys;
