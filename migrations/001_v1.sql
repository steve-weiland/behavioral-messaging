-- V1 schema for behavioral-messaging.
--
-- All entity tables include workspace_id from day one — multi-tenant
-- isolation is a cross-cutting property, not a future tier. Foreign keys
-- to workspaces are intentionally omitted to keep test fixture setup
-- cheap; integrity is enforced at the application layer.
--
-- Mounted into the MySQL container at /docker-entrypoint-initdb.d so
-- it runs once on first start (vanilla mysql:8.0 behaviour). No
-- migration framework — this is V1.

CREATE TABLE IF NOT EXISTS workspaces (
    workspace_id     VARCHAR(64) NOT NULL,
    name             VARCHAR(255) NOT NULL,
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (workspace_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- One row per (workspace, person). Attributes are a JSON blob — V1 stores
-- everything; V2+ may add typed columns for hot fields.
CREATE TABLE IF NOT EXISTS people (
    workspace_id     VARCHAR(64)  NOT NULL,
    person_id        VARCHAR(128) NOT NULL,
    attributes       JSON         NOT NULL,
    created_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (workspace_id, person_id),
    KEY idx_people_workspace_updated (workspace_id, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Append-only events log. (workspace_id, event_id) PK enforces dedup at
-- the DB layer; V1 dispatch isn't idempotent yet, but the hook is here.
CREATE TABLE IF NOT EXISTS events (
    workspace_id     VARCHAR(64)  NOT NULL,
    event_id         VARCHAR(36)  NOT NULL,            -- UUIDv4 from track-api
    person_id        VARCHAR(128) NOT NULL,
    event_name       VARCHAR(128) NOT NULL,
    payload          JSON         NOT NULL,
    received_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (workspace_id, event_id),
    KEY idx_events_workspace_person_received (workspace_id, person_id, received_at),
    KEY idx_events_workspace_name_received (workspace_id, event_name, received_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Audit trail of campaign enrolments. V1 won't have campaigns yet, but
-- the table exists so PR 2+ can write rows without a schema migration.
CREATE TABLE IF NOT EXISTS journey_enrolments (
    workspace_id     VARCHAR(64)  NOT NULL,
    enrolment_id     VARCHAR(36)  NOT NULL,
    campaign_id      VARCHAR(64)  NOT NULL,
    person_id        VARCHAR(128) NOT NULL,
    triggered_by     VARCHAR(36)  NOT NULL,             -- event_id reference
    enrolled_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (workspace_id, enrolment_id),
    KEY idx_enrolments_workspace_campaign (workspace_id, campaign_id, enrolled_at),
    KEY idx_enrolments_workspace_person (workspace_id, person_id, enrolled_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Idempotency-keys table for V2 two-phase dispatch. Empty in V1; rows
-- inserted just before send, updated after ack from delivery provider.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    workspace_id     VARCHAR(64)  NOT NULL,
    idem_key         VARCHAR(128) NOT NULL,             -- e.g. person_id:campaign_id:schedule_key
    status           ENUM('sending','sent','failed') NOT NULL,
    attempt          INT UNSIGNED NOT NULL DEFAULT 1,
    created_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (workspace_id, idem_key),
    KEY idx_idem_workspace_status (workspace_id, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Seed one workspace so tests / curl can use it without a separate setup.
INSERT IGNORE INTO workspaces (workspace_id, name)
VALUES ('ws_alpha', 'Alpha (default development workspace)');

-- Grant the bm user the privileges needed for OTel mysqlreceiver.
-- Created at container init with %, scoped to schema bm. mysqlreceiver
-- queries SHOW GLOBAL STATUS + performance_schema views.
GRANT PROCESS ON *.* TO 'bm'@'%';
GRANT SELECT ON performance_schema.* TO 'bm'@'%';
FLUSH PRIVILEGES;
