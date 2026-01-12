DECLARE since_ts TIMESTAMP DEFAULT @since_ts;

WITH base AS (
  SELECT
    job_id,
    error_result,
    state,
    creation_time,
    start_time,
    end_time
  FROM `region-{{ location }}`.INFORMATION_SCHEMA.JOBS
  WHERE creation_time >= since_ts
    AND statement_type NOT IN ("SCRIPT", "script")
),

running_jobs AS (
  SELECT
    MIN(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, SECOND)) AS min_running_time,
    MAX(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, SECOND)) AS max_running_time,
    AVG(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, SECOND)) AS avg_running_time,
    STDDEV(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, SECOND)) AS stddev_running_time
  FROM base
  WHERE state = 'RUNNING'
)

SELECT
  -- volume metrics
  COUNT(*) AS count_job_submitted,
  COUNTIF(state = 'PENDING') AS count_job_pending,
  COUNTIF(state = 'DONE' AND error_result IS NULL) AS count_job_done,
  COUNTIF(state = 'RUNNING') AS count_job_running,
  COUNTIF(error_result.reason = 'stopped') AS count_job_error,

  -- queueing metrics
  MIN(TIMESTAMP_DIFF(start_time, creation_time, SECOND)) AS min_queueing_time,
  MAX(TIMESTAMP_DIFF(start_time, creation_time, SECOND)) AS max_queueing_time,
  AVG(TIMESTAMP_DIFF(start_time, creation_time, SECOND)) AS avg_queueing_time,
  STDDEV(TIMESTAMP_DIFF(start_time, creation_time, SECOND)) AS stddev_queueing_time,
  APPROX_QUANTILES(
    TIMESTAMP_DIFF(start_time, creation_time, SECOND), 100
  )[OFFSET(99)] AS queueing_time_p99,

  -- running metrics
  running_jobs.min_running_time,
  running_jobs.max_running_time,
  running_jobs.avg_running_time,
  running_jobs.stddev_running_time

FROM base
CROSS JOIN running_jobs;
