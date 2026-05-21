-- V1 PR 4: campaigns table.
--
-- Column name is `trigger_def` (not `trigger`) — `TRIGGER` is a MySQL
-- reserved word for the CREATE TRIGGER DDL; quoting it everywhere is
-- a maintenance trap.
--
-- trigger_def is the segment.Condition JSON tree; validated and
-- decode-tested at insert time so the dispatcher never sees a bad row.
-- template is a Go text/template string; parsed at insert time.

CREATE TABLE IF NOT EXISTS campaigns (
    workspace_id     VARCHAR(64)  NOT NULL,
    campaign_id      VARCHAR(64)  NOT NULL,
    name             VARCHAR(255) NOT NULL,
    trigger_def      JSON         NOT NULL,
    template         TEXT         NOT NULL,
    created_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (workspace_id, campaign_id),
    KEY idx_campaigns_workspace_updated (workspace_id, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
