-- V1 PR 3: segments table.
--
-- definition is the JSON condition tree (see internal/segment/condition.go).
-- V1 stores it as-is and decodes it on every check — V2 may compile +
-- cache, or pre-materialise members into a roaring bitmap.

CREATE TABLE IF NOT EXISTS segments (
    workspace_id     VARCHAR(64)  NOT NULL,
    segment_id       VARCHAR(64)  NOT NULL,
    name             VARCHAR(255) NOT NULL,
    definition       JSON         NOT NULL,
    created_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (workspace_id, segment_id),
    KEY idx_segments_workspace_updated (workspace_id, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
