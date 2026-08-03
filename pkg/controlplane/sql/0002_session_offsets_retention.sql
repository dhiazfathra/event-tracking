-- session_offsets accumulates one row per (tenant, device, session) forever on
-- the hot write path with no cleanup mechanism otherwise. last_seen lets a
-- periodic job (there is no background-job runner in this codebase yet, so
-- this is a documented SQL statement to run on a schedule, not a running
-- scheduler) reclaim rows for sessions that have gone quiet:
--
--   DELETE FROM session_offsets WHERE last_seen < now() - INTERVAL '30 days';
--
-- 30 days comfortably outlives any client's outbox retry window
-- (MaxRetryAttempts in pkg/limits), so a legitimate late retry still finds its
-- original offset.
ALTER TABLE session_offsets
    ADD COLUMN last_seen TIMESTAMPTZ NOT NULL DEFAULT now();
