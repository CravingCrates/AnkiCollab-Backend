use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use crate::database::AppState;
use crate::error::AppError;

/// How long a claim may sit in `running` before another worker takes it over.
/// A worker that dies mid-job leaves its row `running`; without this the deletion
/// would never run and the row would block that user + deck forever.
const STUCK_CLAIM_MINUTES: i32 = 15;

/// Cap on retry backoff. Jobs are delayed, never dropped.
const MAX_BACKOFF_MINUTES: i32 = 6;

const ALERT_AFTER_SECONDS: i64 = 3600;

const FINISHED_JOB_RETENTION_DAYS: i32 = 30;

const WORKER_INTERVAL: Duration = Duration::from_secs(60);
const METRICS_INTERVAL: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleOutcome {
    Scheduled,
    NothingToDelete,
}

#[derive(Debug, Clone)]
pub struct ErasureJob {
    pub id: Uuid,
    pub user_id: i32,
    pub deck_hash: String,
    pub attempts: i32,
}

#[derive(Debug, Clone, Copy)]
pub struct QueueSnapshot {
    pub pending: i64,
    pub running: i64,
    pub oldest_age_seconds: Option<i64>,
}

const DECK_SUBTREE_SQL: &str = "
    WITH RECURSIVE subtree AS (
        SELECT id FROM decks WHERE human_hash = $1
        UNION ALL
        SELECT d.id FROM decks d JOIN subtree s ON d.parent = s.id
    )
    SELECT id FROM subtree
";

const HAS_STATS_SQL: &str = "
    SELECT 1
    FROM note_stats ns
    JOIN notes n ON n.id = ns.note_id
    WHERE ns.user_hash = $1 AND n.deck = ANY($2)
    LIMIT 1
";

const INSERT_JOB_SQL: &str = "
    INSERT INTO erasure_jobs (user_id, deck_hash)
    VALUES ($1, $2)
    ON CONFLICT (user_id, deck_hash) WHERE state IN ('pending', 'running')
    DO NOTHING
    RETURNING id
";

const CLAIM_JOB_SQL: &str = "
    UPDATE erasure_jobs AS job
    SET state = 'running',
        attempts = job.attempts + 1,
        claimed_at = now()
    FROM (
        SELECT id
        FROM erasure_jobs
        WHERE (state = 'pending' AND next_attempt_at <= now())
           OR (state = 'running' AND claimed_at < now() - make_interval(mins => $1))
        ORDER BY requested_at
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    ) AS candidate
    WHERE job.id = candidate.id
    RETURNING job.id, job.user_id, job.deck_hash, job.attempts
";

const QUEUE_SNAPSHOT_SQL: &str = "
    SELECT
        COUNT(*) FILTER (WHERE state = 'pending')::bigint,
        COUNT(*) FILTER (WHERE state = 'running')::bigint,
        EXTRACT(EPOCH FROM now() - MIN(requested_at)
                FILTER (WHERE state IN ('pending', 'running')))::bigint
    FROM erasure_jobs
";

fn backoff_minutes(attempts: i32) -> i32 {
    attempts.clamp(1, MAX_BACKOFF_MINUTES)
}

async fn deck_subtree(
    client: &tokio_postgres::Client,
    deck_hash: &str,
) -> Result<Vec<i64>, AppError> {
    let rows = client
        .query(DECK_SUBTREE_SQL, &[&deck_hash])
        .await
        .map_err(|err| AppError::db_query("delete_deck_stats", err))?;
    Ok(rows.iter().map(|row| row.get(0)).collect())
}

pub async fn schedule(
    db_state: &Arc<AppState>,
    user_id: i32,
    deck_hash: &str,
) -> Result<ScheduleOutcome, AppError> {
    let user_hash = crate::auth::get_user_hash_by_user_id(db_state, user_id)
        .await
        .map_err(|err| {
            AppError::bug(
                "delete_deck_stats",
                format!("Could not resolve a user hash for user {user_id}: {err}"),
            )
        })?;

    let client = db_state
        .db_pool
        .get()
        .await
        .map_err(|err| AppError::db_connection("delete_deck_stats", err))?;

    let deck_ids = deck_subtree(&client, deck_hash).await?;
    if deck_ids.is_empty() {
        return Ok(ScheduleOutcome::NothingToDelete);
    }

    let has_rows = client
        .query_opt(HAS_STATS_SQL, &[&user_hash, &deck_ids])
        .await
        .map_err(|err| AppError::db_query("delete_deck_stats", err))?
        .is_some();
    if !has_rows {
        return Ok(ScheduleOutcome::NothingToDelete);
    }

    let inserted = client
        .query_opt(INSERT_JOB_SQL, &[&user_id, &deck_hash])
        .await
        .map_err(|err| AppError::db_query("delete_deck_stats", err))?;

    match inserted {
        Some(row) => {
            let job_id: Uuid = row.get(0);
            tracing::info!(
                job_id = %job_id,
                user_id,
                deck_hash = %deck_hash,
                event = "erasure_requested",
                "Deck stats erasure scheduled"
            );
        }
        None => {
            tracing::info!(
                user_id,
                deck_hash = %deck_hash,
                event = "erasure_requested",
                "Deck stats erasure already queued"
            );
        }
    }

    Ok(ScheduleOutcome::Scheduled)
}

async fn claim(client: &tokio_postgres::Client) -> Result<Option<ErasureJob>, AppError> {
    let row = client
        .query_opt(CLAIM_JOB_SQL, &[&STUCK_CLAIM_MINUTES])
        .await
        .map_err(|err| AppError::db_query("erasure_worker", err))?;
    Ok(row.map(|row| ErasureJob {
        id: row.get(0),
        user_id: row.get(1),
        deck_hash: row.get(2),
        attempts: row.get(3),
    }))
}

pub async fn queue_snapshot(db_state: &Arc<AppState>) -> Result<QueueSnapshot, AppError> {
    let client = db_state
        .db_pool
        .get()
        .await
        .map_err(|err| AppError::db_connection("erasure_metrics", err))?;
    let row = client
        .query_one(QUEUE_SNAPSHOT_SQL, &[])
        .await
        .map_err(|err| AppError::db_query("erasure_metrics", err))?;
    Ok(QueueSnapshot {
        pending: row.get(0),
        running: row.get(1),
        oldest_age_seconds: row.get(2),
    })
}

pub async fn report_queue_health(db_state: &Arc<AppState>) {
    match queue_snapshot(db_state).await {
        Ok(snapshot) => {
            if snapshot.pending != 0 || snapshot.running != 0 {
                println!(
                    "Erasure queue: pending={} running={} oldest_pending_age_secs={:?}",
                    snapshot.pending, snapshot.running, snapshot.oldest_age_seconds
                );
            }

            let oldest_age = snapshot.oldest_age_seconds.unwrap_or(0);
            if oldest_age > ALERT_AFTER_SECONDS {
                AppError::bug(
                    "erasure_worker",
                    format!(
                        "Oldest deck stats erasure job has been active for {oldest_age}s \
                         (pending={}, running={}). Withdrawals are not being honoured.",
                        snapshot.pending, snapshot.running
                    ),
                )
                .report_to_sentry();
            }
        }
        Err(err) => err.report_to_sentry(),
    }
}

pub async fn prune_finished(client: &tokio_postgres::Client) -> Result<u64, tokio_postgres::Error> {
    client
        .execute(
            "DELETE FROM erasure_jobs
             WHERE state IN ('done', 'failed')
               AND COALESCE(finished_at, requested_at) < now() - make_interval(days => $1)",
            &[&FINISHED_JOB_RETENTION_DAYS],
        )
        .await
}

const DELETE_STATS_SQL: &str = "
    DELETE FROM note_stats ns
    USING notes n
    WHERE ns.note_id = n.id
      AND ns.user_hash = $1
      AND n.deck = ANY($2)
    RETURNING ns.note_id
";

const DELETE_CALCULATED_SQL: &str = "DELETE FROM calculated_stats WHERE note_id = ANY($1)";

const RECALCULATE_NOTES_SQL: &str = "
    INSERT INTO calculated_stats (note_id, sample_size, retention, lapses, reps)
    SELECT ns.note_id,
           COUNT(DISTINCT ns.user_hash),
           ROUND(AVG(ns.retention), 1),
           ROUND(AVG(ns.lapses), 1),
           ROUND(AVG(ns.reps), 1)
    FROM note_stats ns
    WHERE ns.note_id = ANY($1)
    GROUP BY ns.note_id
";

const AFFECTED_DECKS_SQL: &str = "
    WITH RECURSIVE affected AS (
        SELECT d.id, d.parent, 0 AS depth
        FROM decks d
        WHERE d.id IN (SELECT deck FROM notes WHERE id = ANY($1))
        UNION ALL
        SELECT d.id, d.parent, affected.depth + 1
        FROM decks d JOIN affected ON d.id = affected.parent
    )
    SELECT id, MAX(depth) FROM affected GROUP BY id ORDER BY MAX(depth) ASC
";

const UPDATE_NOTE_COUNT_SQL: &str = "
    UPDATE decks d
    SET notes_with_stats_count = COALESCE(
            (SELECT SUM(c.notes_with_stats_count) FROM decks c WHERE c.parent = d.id), 0
        ) + (
            SELECT COUNT(*)
            FROM notes n
            JOIN calculated_stats cs ON n.id = cs.note_id
            WHERE n.deck = d.id AND NOT n.deleted
        )
    WHERE d.id = ANY($1::bigint[])
";

const UPDATE_RETENTION_SQL: &str = "
    UPDATE decks d
    SET retention = computed.retention
    FROM (
        SELECT t.id AS deck_id,
               CASE
                   WHEN SUM(x.note_count) = 0 THEN NULL
                   ELSE CAST(ROUND((SUM(x.retention * x.note_count) / SUM(x.note_count))::numeric, 1) AS REAL)
               END AS retention
        FROM UNNEST($1::bigint[]) AS t(id)
        LEFT JOIN LATERAL (
            SELECT cs.retention, 1 AS note_count
            FROM notes n
            JOIN calculated_stats cs ON n.id = cs.note_id
            WHERE n.deck = t.id AND NOT n.deleted AND cs.retention IS NOT NULL
            UNION ALL
            SELECT c.retention, c.notes_with_stats_count AS note_count
            FROM decks c
            WHERE c.parent = t.id AND c.retention IS NOT NULL
        ) AS x ON TRUE
        GROUP BY t.id
    ) AS computed
    WHERE d.id = computed.deck_id
";

async fn claim_next(db_state: &Arc<AppState>) -> Result<Option<ErasureJob>, AppError> {
    let client = db_state
        .db_pool
        .get()
        .await
        .map_err(|err| AppError::db_connection("erasure_worker", err))?;
    claim(&client).await
}

fn level_buckets(pairs: impl IntoIterator<Item = (i64, i32)>) -> Vec<Vec<i64>> {
    let mut levels: BTreeMap<i32, Vec<i64>> = BTreeMap::new();
    for (deck_id, depth) in pairs {
        levels.entry(depth).or_default().push(deck_id);
    }
    levels.into_values().collect()
}

async fn refresh_deck_aggregates(
    tx: &tokio_postgres::Transaction<'_>,
    deleted_note_ids: &[i64],
) -> Result<(), AppError> {
    let rows = tx
        .query(AFFECTED_DECKS_SQL, &[&deleted_note_ids])
        .await
        .map_err(|err| AppError::db_query("erasure_worker", err))?;

    for deck_ids in level_buckets(rows.iter().map(|row| (row.get(0), row.get(1)))) {
        tx.execute(UPDATE_NOTE_COUNT_SQL, &[&deck_ids])
            .await
            .map_err(|err| AppError::db_query("erasure_worker", err))?;

        tx.execute(UPDATE_RETENTION_SQL, &[&deck_ids])
            .await
            .map_err(|err| AppError::db_query("erasure_worker", err))?;
    }

    Ok(())
}

async fn execute(db_state: &Arc<AppState>, job: &ErasureJob) -> Result<usize, AppError> {
    let user_hash = crate::auth::get_user_hash_by_user_id(db_state, job.user_id)
        .await
        .map_err(|err| {
            AppError::bug(
                "erasure_worker",
                format!(
                    "Could not resolve a user hash for user {}: {err}",
                    job.user_id
                ),
            )
        })?;

    let mut client = db_state
        .db_pool
        .get()
        .await
        .map_err(|err| AppError::db_connection("erasure_worker", err))?;

    let deck_ids = deck_subtree(&client, &job.deck_hash).await?;
    if deck_ids.is_empty() {
        return Ok(0);
    }

    let tx = client
        .transaction()
        .await
        .map_err(|err| AppError::db_query("erasure_worker", err))?;

    let deleted_note_ids: Vec<i64> = tx
        .query(DELETE_STATS_SQL, &[&user_hash, &deck_ids])
        .await
        .map_err(|err| AppError::db_query("erasure_worker", err))?
        .iter()
        .map(|row| row.get(0))
        .collect();

    if !deleted_note_ids.is_empty() {
        tx.execute(DELETE_CALCULATED_SQL, &[&deleted_note_ids])
            .await
            .map_err(|err| AppError::db_query("erasure_worker", err))?;
        tx.execute(RECALCULATE_NOTES_SQL, &[&deleted_note_ids])
            .await
            .map_err(|err| AppError::db_query("erasure_worker", err))?;
        refresh_deck_aggregates(&tx, &deleted_note_ids).await?;
    }

    tx.commit()
        .await
        .map_err(|err| AppError::db_query("erasure_worker", err))?;

    Ok(deleted_note_ids.len())
}

async fn mark_done(db_state: &Arc<AppState>, job_id: Uuid) -> Result<bool, AppError> {
    let client = db_state
        .db_pool
        .get()
        .await
        .map_err(|err| AppError::db_connection("erasure_worker", err))?;
    let updated = client
        .execute(
            "UPDATE erasure_jobs
             SET state = 'done', claimed_at = NULL, finished_at = now(), last_error = NULL
             WHERE id = $1 AND state = 'running'",
            &[&job_id],
        )
        .await
        .map_err(|err| AppError::db_query("erasure_worker", err))?;
    Ok(updated > 0)
}

async fn reschedule(
    db_state: &Arc<AppState>,
    job: &ErasureJob,
    last_error: &str,
) -> Result<(), AppError> {
    let client = db_state
        .db_pool
        .get()
        .await
        .map_err(|err| AppError::db_connection("erasure_worker", err))?;
    client
        .execute(
            "UPDATE erasure_jobs
             SET state = 'pending',
                 claimed_at = NULL,
                 last_error = $2,
                 next_attempt_at = now() + make_interval(mins => $3)
             WHERE id = $1 AND state = 'running'",
            &[&job.id, &last_error, &backoff_minutes(job.attempts)],
        )
        .await
        .map_err(|err| AppError::db_query("erasure_worker", err))?;
    Ok(())
}

pub async fn process_pending_jobs(db_state: &Arc<AppState>) -> usize {
    let mut completed = 0;

    loop {
        let job = match claim_next(db_state).await {
            Ok(Some(job)) => job,
            Ok(None) => break,
            Err(err) => {
                err.report_to_sentry();
                break;
            }
        };

        match execute(db_state, &job).await {
            Ok(deleted_rows) => match mark_done(db_state, job.id).await {
                Ok(true) => {
                    completed += 1;
                    tracing::info!(
                        job_id = %job.id,
                        user_id = job.user_id,
                        deck_hash = %job.deck_hash,
                        deleted_note_rows = deleted_rows,
                        event = "erasure_completed",
                        "Deck stats erasure finished"
                    );
                }
                Ok(false) => {}
                Err(err) => err.report_to_sentry(),
            },
            Err(err) => {
                let detail: String = err
                    .internal_message
                    .clone()
                    .unwrap_or_else(|| err.message.clone())
                    .chars()
                    .take(1000)
                    .collect();
                tracing::warn!(
                    job_id = %job.id,
                    user_id = job.user_id,
                    deck_hash = %job.deck_hash,
                    attempts = job.attempts,
                    error = %detail,
                    event = "erasure_failed",
                    "Deck stats erasure failed and will be retried"
                );
                if let Err(reschedule_err) = reschedule(db_state, &job, &detail).await {
                    reschedule_err.report_to_sentry();
                }
            }
        }
    }

    completed
}

pub fn start_worker(state: Arc<AppState>) {
    let worker_state = Arc::clone(&state);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(WORKER_INTERVAL);
        loop {
            ticker.tick().await;
            process_pending_jobs(&worker_state).await;
        }
    });

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(METRICS_INTERVAL);
        loop {
            ticker.tick().await;
            report_queue_health(&state).await;
        }
    });
}
