-- V3-1a: multi-step journeys (BM-110..114).
--
-- A `journey` is a trigger + an ordered step list. A `journey_run` is one
-- person's position in one journey — the unit the V3-1b scheduler claims and
-- advances. A V2 campaign is the degenerate case of a journey: a single send
-- step that completes on creation.

CREATE TABLE IF NOT EXISTS journeys (
    workspace_id  VARCHAR(64)  NOT NULL,
    journey_id    VARCHAR(64)  NOT NULL,
    name          VARCHAR(255) NOT NULL,
    trigger_def   JSON         NOT NULL,            -- segment.Condition tree (BM-31 grammar)
    steps         JSON         NOT NULL,            -- ordered array; 3 step types (BM-111)
    -- Runs pin the version they started on (BM-114), so editing a journey
    -- never mutates someone already halfway through it.
    version       INT UNSIGNED NOT NULL DEFAULT 1,
    created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (workspace_id, journey_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS journey_runs (
    workspace_id      VARCHAR(64)  NOT NULL,
    run_id            VARCHAR(36)  NOT NULL,
    journey_id        VARCHAR(64)  NOT NULL,
    journey_version   INT UNSIGNED NOT NULL,        -- pinned at creation (BM-114)
    person_id         VARCHAR(128) NOT NULL,
    triggered_by      VARCHAR(36)  NOT NULL,        -- event_id that enrolled them
    step_index        INT UNSIGNED NOT NULL DEFAULT 0,
    -- ready   : due now, never claimed
    -- waiting : sleeping until wake_at (a delay, or a retry backoff)
    -- running : claimed by a scheduler, lease live until claim_expires_at
    -- done / failed / cancelled : terminal
    status            ENUM('ready','waiting','running','done','failed','cancelled')
                      NOT NULL DEFAULT 'ready',
    wake_at           DATETIME     NULL,            -- NULL = immediately due
    attempt           INT UNSIGNED NOT NULL DEFAULT 0,
    last_error        VARCHAR(512) NULL,
    -- Lease (BM-117): a run whose claim_expires_at has passed is reclaimable
    -- by any scheduler, which is what makes scheduler death recoverable
    -- without holding a transaction across the send.
    claimed_by        VARCHAR(64)  NULL,
    claim_expires_at  DATETIME     NULL,
    created_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (workspace_id, run_id),
    -- BM-113: at-least-once delivery means the same trigger event can arrive
    -- twice. One run per (journey, triggering event) makes the enrolling
    -- INSERT ... ON DUPLICATE KEY UPDATE a no-op on redelivery — the same
    -- mechanism V2-3 used for journey_enrollments. Note this permits re-entry
    -- on a DIFFERENT event, which is the right default for abandoned-cart and
    -- the wrong one for onboarding; see spec Q15.
    UNIQUE KEY uniq_run_dispatch (workspace_id, journey_id, triggered_by),
    -- The scheduler's due query (BM-115). status first so the range scan on
    -- wake_at only walks rows that could be due; workspace_id trails so the
    -- V3-3 fair-claim variant can filter on it without a second index.
    KEY idx_runs_due (status, wake_at, workspace_id),
    KEY idx_runs_person (workspace_id, person_id, journey_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
