-- V2-3: make the journey_enrollments audit insert idempotent.
--
-- The PK is (workspace_id, enrollment_id), but enrollment_id is a fresh
-- UUID per dispatch, so an at-least-once redelivery (crash/nack after the
-- stub POST but before ack) would create a DUPLICATE enrollment for the
-- same (campaign, event). This unique natural key — "this campaign fired
-- for this inbound event" — collapses redeliveries: the batched writer
-- uses INSERT ... ON DUPLICATE KEY UPDATE, so a replay is a no-op.
--
-- triggered_by is the event_id; (workspace_id, campaign_id, triggered_by)
-- is exactly one row per (campaign, event). Runs on the empty table at
-- fresh `make up`, so the constraint holds from init.
ALTER TABLE journey_enrollments
    ADD UNIQUE KEY uniq_enrollment_dispatch (workspace_id, campaign_id, triggered_by);
