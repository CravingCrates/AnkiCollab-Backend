//! Contributor impact statistics served by `GET /GetMyImpactStats`.

use crate::{database, structs};

use serde::Serialize;
use std::sync::Arc;

/// Reviewed suggestions needed before a rate is shown: one or two decisions are noise.
pub const MIN_REVIEWED_FOR_RATE: i64 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MilestoneTier {
    pub threshold: i64,
    pub name: &'static str,
}

pub const MILESTONES: [MilestoneTier; 8] = [
    MilestoneTier {
        threshold: 1,
        name: "Contributor",
    },
    MilestoneTier {
        threshold: 5,
        name: "Regular Contributor",
    },
    MilestoneTier {
        threshold: 10,
        name: "Trusted Contributor",
    },
    MilestoneTier {
        threshold: 25,
        name: "Core Contributor",
    },
    MilestoneTier {
        threshold: 50,
        name: "Deck Steward",
    },
    MilestoneTier {
        threshold: 100,
        name: "Community Steward",
    },
    MilestoneTier {
        threshold: 250,
        name: "Community Pillar",
    },
    MilestoneTier {
        threshold: 500,
        name: "Cornerstone",
    },
];

#[derive(Debug, Clone, Copy)]
pub struct MilestoneProgress {
    pub accepted: i64,
    pub current: Option<MilestoneTier>,
    pub next: Option<MilestoneTier>,
    pub progress_to_next: Option<f64>,
    pub remaining_to_next: Option<i64>,
}

/// `None` when fewer than [`MIN_REVIEWED_FOR_RATE`] suggestions were reviewed.
#[must_use]
pub fn acceptance_rate(reviewed: i64, accepted: i64) -> Option<f64> {
    if reviewed < MIN_REVIEWED_FOR_RATE {
        return None;
    }
    Some(accepted.clamp(0, reviewed) as f64 / reviewed as f64)
}

#[must_use]
pub fn reach(accepted: i64, subscribers: i64) -> i64 {
    if accepted <= 0 || subscribers <= 0 {
        return 0;
    }
    accepted.saturating_mul(subscribers)
}

#[must_use]
pub fn milestone_progress(accepted: i64) -> MilestoneProgress {
    let accepted = accepted.max(0);
    let current = MILESTONES
        .iter()
        .rev()
        .find(|tier| tier.threshold <= accepted)
        .copied();
    let next = MILESTONES
        .iter()
        .find(|tier| tier.threshold > accepted)
        .copied();

    let (progress_to_next, remaining_to_next) = match (current, next) {
        (Some(current), Some(next)) => (
            Some(
                (accepted - current.threshold) as f64 / (next.threshold - current.threshold) as f64,
            ),
            Some(next.threshold - accepted),
        ),
        (None, Some(next)) => (Some(0.0), Some(next.threshold - accepted)),
        _ => (None, None),
    };

    MilestoneProgress {
        accepted,
        current,
        next,
        progress_to_next,
        remaining_to_next,
    }
}

pub async fn get_my_impact(
    db_state: &Arc<database::AppState>,
    user_id: i32,
    user_hash: &str,
) -> Result<structs::MyImpactStats, Box<dyn std::error::Error + Send + Sync>> {
    use std::collections::HashMap;

    let now = chrono::Utc::now();
    let client = db_state.db_pool.get().await?;

    let sub_rows = client
        .query(
            "SELECT s.deck_id,
                    s.created_at,
                    d.full_path,
                    d.name,
                    COALESCE(d.human_hash, '')
               FROM anki.subscriptions s
               JOIN anki.decks d ON d.id = s.deck_id
              WHERE s.user_hash = $1
                AND d.deleted_at IS NULL
                AND d.human_hash IS NOT NULL
              ORDER BY s.created_at DESC NULLS LAST, d.full_path",
            &[&user_hash],
        )
        .await?;

    let deck_ids: Vec<i64> = sub_rows.iter().map(|row| row.get(0)).collect();

    // submitted = accepted + declined + pending + unrecorded. `unrecorded` exists because
    // per-field/per-tag reviews write no notifications row, so no verdict is recorded for them.
    let mine_rows = client
        .query(
            "WITH sugg AS (
                 SELECT c.commit_id,
                        c.deck AS deck_id,
                        (EXISTS (SELECT 1 FROM anki.fields f
                                  WHERE f.commit = c.commit_id AND f.reviewed = false)
                      OR EXISTS (SELECT 1 FROM anki.tags t
                                  WHERE t.commit = c.commit_id AND t.reviewed = false)
                      OR EXISTS (SELECT 1 FROM anki.card_deletion_suggestions cds
                                  WHERE cds.commit = c.commit_id)
                      OR EXISTS (SELECT 1 FROM anki.note_move_suggestions nms
                                  WHERE nms.commit = c.commit_id)) AS pending
                   FROM anki.commits c
                  WHERE c.user_id = $1 AND c.deck IS NOT NULL
             ),
             verdicts AS (
                 SELECT n.commit_id,
                        bool_or(n.status = 'approved') AS approved,
                        bool_or(n.status = 'denied')   AS denied
                   FROM anki.notifications n
                  WHERE n.user_id = $1
                  GROUP BY n.commit_id
             )
             SELECT s.deck_id,
                    COUNT(*)::bigint AS submitted,
                    COUNT(*) FILTER (WHERE COALESCE(v.approved, false))::bigint AS accepted,
                    COUNT(*) FILTER (WHERE COALESCE(v.denied, false)
                                       AND NOT COALESCE(v.approved, false))::bigint AS declined,
                    COUNT(*) FILTER (WHERE s.pending
                                       AND NOT COALESCE(v.approved, false)
                                       AND NOT COALESCE(v.denied, false))::bigint AS pending,
                    COUNT(*) FILTER (WHERE NOT s.pending
                                       AND NOT COALESCE(v.approved, false)
                                       AND NOT COALESCE(v.denied, false))::bigint AS unrecorded
               FROM sugg s
               LEFT JOIN verdicts v ON v.commit_id = s.commit_id
              GROUP BY s.deck_id",
            &[&user_id],
        )
        .await?;

    let mut mine_by_deck: HashMap<i64, MineCounts> = HashMap::new();
    for row in &mine_rows {
        mine_by_deck.insert(
            row.get(0),
            MineCounts {
                submitted: row.get(1),
                accepted: row.get(2),
                declined: row.get(3),
                pending: row.get(4),
                unrecorded: row.get(5),
            },
        );
    }

    // Counted over each deck's SUBTREE so it agrees with the snapshot-based net figure beside it;
    // this is a different number from the direct count /decks publishes.
    let subscriber_rows = client
        .query(
            "WITH RECURSIVE tree AS (
                 SELECT id AS root_id, id AS node_id
                   FROM anki.decks
                  WHERE id = ANY($1::bigint[])
                 UNION
                 SELECT t.root_id, d.id
                   FROM tree t
                   JOIN anki.decks d ON d.parent = t.node_id
                  WHERE d.deleted_at IS NULL
             )
             SELECT t.root_id AS deck_id, COUNT(s.id)::bigint
               FROM tree t
               LEFT JOIN anki.subscriptions s ON s.deck_id = t.node_id
              GROUP BY t.root_id",
            &[&deck_ids],
        )
        .await?;
    let subscribers_by_deck: HashMap<i64, i64> = subscriber_rows
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();

    // Over each deck's SUBTREE, like the subscriber count beside it and like the website's figure:
    // an acceptance on a subdeck belongs to the deck's community.
    let accepted_30d_rows = client
        .query(
            "WITH RECURSIVE tree AS (
                 SELECT id AS root_id, id AS node_id
                   FROM anki.decks
                  WHERE id = ANY($1::bigint[])
                 UNION
                 SELECT t.root_id, d.id
                   FROM tree t
                   JOIN anki.decks d ON d.parent = t.node_id
                  WHERE d.deleted_at IS NULL
             )
             SELECT t.root_id AS deck_id, COUNT(DISTINCT n.commit_id)::bigint
               FROM tree t
               JOIN anki.notifications n ON n.deck_id = t.node_id
              WHERE n.status = 'approved'
                AND n.created_at >= now() - INTERVAL '30 days'
              GROUP BY t.root_id",
            &[&deck_ids],
        )
        .await?;
    let accepted_30d_by_deck: HashMap<i64, i64> = accepted_30d_rows
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();

    let tracking_rows = client
        .query(
            "SELECT deck_id, TO_CHAR(MIN(captured_on), 'YYYY-MM-DD')
               FROM anki.deck_stats_daily
              WHERE deck_id = ANY($1::bigint[])
              GROUP BY deck_id",
            &[&deck_ids],
        )
        .await?;
    let tracking_since_by_deck: HashMap<i64, String> = tracking_rows
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();

    let latest_rows = client
        .query(
            "SELECT DISTINCT ON (deck_id)
                    deck_id,
                    subscriber_count::bigint,
                    (captured_on >= CURRENT_DATE - 7) AS is_fresh
               FROM anki.deck_stats_daily
              WHERE deck_id = ANY($1::bigint[])
              ORDER BY deck_id, captured_on DESC",
            &[&deck_ids],
        )
        .await?;
    let latest_by_deck: HashMap<i64, (i64, bool)> = latest_rows
        .iter()
        .map(|row| (row.get(0), (row.get(1), row.get(2))))
        .collect();

    let baseline_rows = client
        .query(
            "SELECT DISTINCT ON (deck_id) deck_id, subscriber_count::bigint
               FROM anki.deck_stats_daily
              WHERE deck_id = ANY($1::bigint[])
                AND captured_on <= CURRENT_DATE - 30
              ORDER BY deck_id, captured_on DESC",
            &[&deck_ids],
        )
        .await?;
    let baseline_by_deck: HashMap<i64, i64> = baseline_rows
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();

    let latest_row = client
        .query_opt(
            "SELECT n.commit_id,
                    TO_CHAR(n.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS+00:00'),
                    d.full_path,
                    d.name,
                    u.username
               FROM anki.notifications n
               JOIN anki.commits c ON c.commit_id = n.commit_id
               JOIN anki.decks d ON d.id = n.deck_id
               LEFT JOIN public.users u ON u.id = n.actor_user_id
              WHERE n.user_id = $1
                AND n.status = 'approved'
                AND d.deleted_at IS NULL
              ORDER BY n.created_at DESC, n.id DESC
              LIMIT 1",
            &[&user_id],
        )
        .await?;

    let mut totals = MineCounts::default();
    let mut total_reach: i64 = 0;
    let mut decks: Vec<structs::ImpactDeck> = Vec::with_capacity(sub_rows.len());

    for row in &sub_rows {
        let deck_id: i64 = row.get(0);
        let created_at: Option<chrono::DateTime<chrono::Utc>> = row.get(1);
        let full_path: Option<String> = row.get(2);
        let plain_name: Option<String> = row.get(3);
        let human_hash: String = row.get(4);

        let mine = mine_by_deck.get(&deck_id).copied().unwrap_or_default();
        let subscribers = subscribers_by_deck.get(&deck_id).copied().unwrap_or(0);
        let mine_reach = reach(mine.accepted, subscribers);

        let latest_snapshot = latest_by_deck.get(&deck_id).copied();
        let net_subscribers_30d = match (latest_snapshot, baseline_by_deck.get(&deck_id)) {
            (Some((latest, true)), Some(baseline)) => Some(latest - baseline),
            _ => None,
        };

        decks.push(structs::ImpactDeck {
            deck_hash: human_hash,
            deck_name: deck_display_name(full_path.as_deref(), plain_name.as_deref()),
            subscribed_at: created_at.map(|dt| dt.to_rfc3339()),
            subscribed_days: created_at.map(|dt| days_since(dt, now)),
            community: structs::ImpactDeckCommunity {
                subscribers,
                net_subscribers_30d,
                accepted_30d: accepted_30d_by_deck.get(&deck_id).copied().unwrap_or(0),
                tracking_since: tracking_since_by_deck.get(&deck_id).cloned(),
            },
            mine: structs::ImpactCounts {
                submitted: mine.submitted,
                accepted: mine.accepted,
                declined: mine.declined,
                pending: mine.pending,
                unrecorded: mine.unrecorded,
                reviewed: mine.reviewed(),
                acceptance_rate: acceptance_rate(mine.reviewed(), mine.accepted),
                reach: mine_reach,
            },
        });

        total_reach = total_reach.saturating_add(mine_reach);
    }

    // Lifetime, like `decks_contributed_to`: unsubscribing must not erase what someone contributed.
    for mine in mine_by_deck.values() {
        totals.add(mine);
    }

    let decks_contributed_to = i64::try_from(
        mine_by_deck
            .values()
            .filter(|mine| mine.submitted > 0)
            .count(),
    )
    .unwrap_or(i64::MAX);

    let reviewed_total = totals.reviewed();
    let milestone = milestone_progress(totals.accepted);
    let tracking_since = tracking_since_by_deck.values().min().cloned();

    Ok(structs::MyImpactStats {
        generated_at: now.to_rfc3339(),
        tracking_since,
        totals: structs::ImpactTotals {
            submitted: totals.submitted,
            accepted: totals.accepted,
            declined: totals.declined,
            pending: totals.pending,
            unrecorded: totals.unrecorded,
            reviewed: reviewed_total,
            acceptance_rate: acceptance_rate(reviewed_total, totals.accepted),
            reach: total_reach,
            decks_contributed_to,
        },
        milestone: structs::ImpactMilestone {
            accepted: milestone.accepted,
            current: milestone.current,
            next: milestone.next,
            progress_to_next: milestone.progress_to_next,
            remaining_to_next: milestone.remaining_to_next,
        },
        latest_accepted: latest_row.map(|row| {
            let full_path: Option<String> = row.get(2);
            let plain_name: Option<String> = row.get(3);
            structs::ImpactLatestAccepted {
                deck_name: deck_display_name(full_path.as_deref(), plain_name.as_deref()),
                commit_id: row.get(0),
                accepted_at: row.get(1),
                accepted_by: row.get(4),
            }
        }),
        decks,
    })
}

#[derive(Debug, Default, Clone, Copy)]
struct MineCounts {
    submitted: i64,
    accepted: i64,
    declined: i64,
    pending: i64,
    unrecorded: i64,
}

impl MineCounts {
    const fn reviewed(&self) -> i64 {
        self.accepted + self.declined
    }

    fn add(&mut self, other: &Self) {
        self.submitted += other.submitted;
        self.accepted += other.accepted;
        self.declined += other.declined;
        self.pending += other.pending;
        self.unrecorded += other.unrecorded;
    }
}

#[must_use]
pub fn deck_display_name(full_path: Option<&str>, name: Option<&str>) -> String {
    full_path
        .and_then(|path| path.rsplit("::").next())
        .map(str::trim)
        .filter(|leaf| !leaf.is_empty())
        .or_else(|| name.map(str::trim).filter(|name| !name.is_empty()))
        .unwrap_or("[Unnamed Deck]")
        .to_string()
}

#[must_use]
pub fn days_since(then: chrono::DateTime<chrono::Utc>, now: chrono::DateTime<chrono::Utc>) -> i64 {
    (now - then).num_days().max(0)
}

const SNAPSHOT_SQL: &str = "
WITH RECURSIVE
    needed AS (
        SELECT deck_id AS id FROM anki.subscriptions
        UNION
        SELECT deck_id FROM anki.deck_stats_daily
    ),
    upwards AS (
        SELECT n.id AS node_id FROM needed n
        UNION
        SELECT d.parent
          FROM upwards u
          JOIN anki.decks d ON d.id = u.node_id
         WHERE d.parent IS NOT NULL
    ),
    tree_nodes AS (
        SELECT DISTINCT u.node_id AS deck_id
          FROM upwards u
          JOIN anki.decks d ON d.id = u.node_id
         WHERE d.deleted_at IS NULL
    ),
    direct AS MATERIALIZED (
        SELECT s.deck_id,
               d.parent,
               COUNT(*)::bigint AS subs,
               COUNT(*) FILTER (WHERE s.last_synced_at >= now() - INTERVAL '30 days')::bigint AS a30,
               COUNT(*) FILTER (WHERE s.last_synced_at >= now() - INTERVAL '90 days')::bigint AS a90
          FROM anki.subscriptions s
          JOIN anki.decks d ON d.id = s.deck_id
         WHERE d.deleted_at IS NULL
         GROUP BY s.deck_id, d.parent
    ),
    ancestry AS (
        SELECT deck_id AS origin, deck_id AS node_id, parent, subs, a30, a90 FROM direct
        UNION
        SELECT a.origin, p.id, p.parent, a.subs, a.a30, a.a90
          FROM ancestry a
          JOIN anki.decks p ON p.id = a.parent
         WHERE p.deleted_at IS NULL
    ),
    rolled AS (
        SELECT a.node_id AS root_id,
               SUM(a.subs) AS subs,
               SUM(a.a30)  AS a30,
               SUM(a.a90)  AS a90
          FROM ancestry a
         GROUP BY a.node_id
    )
    INSERT INTO anki.deck_stats_daily (deck_id, captured_on, subscriber_count, active_30d, active_90d)
    SELECT t.deck_id,
           CURRENT_DATE,
           COALESCE(r.subs, 0),
           COALESCE(r.a30, 0),
           COALESCE(r.a90, 0)
      FROM tree_nodes t
      LEFT JOIN rolled r ON r.root_id = t.deck_id
    ON CONFLICT (deck_id, captured_on) DO UPDATE
       SET subscriber_count = EXCLUDED.subscriber_count,
           active_30d       = EXCLUDED.active_30d,
           active_90d       = EXCLUDED.active_90d
";

pub async fn snapshot_deck_subscribers(
    client: &database::SharedConn,
) -> Result<u64, tokio_postgres::Error> {
    client.execute(SNAPSHOT_SQL, &[]).await
}

/// Upserts because a deck delete cascades the subscription rows away, so a re-upload under the same
/// hash leaves the client pulling with no row; rows it creates get `created_at = NULL`.
pub async fn touch_last_synced(
    db_state: &Arc<database::AppState>,
    user_id: i32,
    deck_hashes: &[String],
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    if deck_hashes.is_empty() {
        return Ok(0);
    }

    // Derive through `auth`: a second hand-written copy of SHA256(username) is how a subscription
    // silently stops matching its owner.
    let client = db_state.db_pool.get().await?;
    let username_row = client
        .query_opt(
            "SELECT username FROM public.users WHERE id = $1",
            &[&user_id],
        )
        .await?;
    let Some(username_row) = username_row else {
        return Ok(0);
    };
    let username: String = username_row.get(0);
    let user_hash = crate::auth::user_hash_from_username(&username);

    let touched = client
        .execute(
            "INSERT INTO anki.subscriptions (deck_id, user_hash, last_synced_at)
             SELECT d.id, $1, now()
               FROM anki.decks d
              WHERE d.human_hash = ANY($2::text[])
                AND d.deleted_at IS NULL
             ON CONFLICT (user_hash, deck_id)
             DO UPDATE SET last_synced_at = now()",
            &[&user_hash, &deck_hashes],
        )
        .await?;

    Ok(touched)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(actual: f64, expected: f64) -> bool {
        (actual - expected).abs() < 1e-9
    }

    #[test]
    fn acceptance_rate_is_hidden_with_no_reviews() {
        assert_eq!(acceptance_rate(0, 0), None);
    }

    #[test]
    fn acceptance_rate_is_hidden_below_five_reviews() {
        assert_eq!(acceptance_rate(4, 4), None);
        assert_eq!(acceptance_rate(4, 0), None);
    }

    #[test]
    fn acceptance_rate_appears_at_five_reviews() {
        let rate = acceptance_rate(5, 4).expect("5 reviewed is enough to show a rate");
        assert!(close(rate, 0.8), "expected 0.8, got {rate}");
    }

    #[test]
    fn acceptance_rate_handles_the_extremes() {
        assert!(close(acceptance_rate(5, 5).unwrap(), 1.0));
        assert!(close(acceptance_rate(10, 0).unwrap(), 0.0));
    }

    #[test]
    fn acceptance_rate_never_exceeds_one_on_inconsistent_data() {
        assert!(close(acceptance_rate(5, 7).unwrap(), 1.0));
    }

    #[test]
    fn reach_multiplies_accepted_by_subscribers() {
        assert_eq!(reach(3, 100), 300);
    }

    #[test]
    fn reach_is_zero_when_nothing_was_accepted_or_nobody_subscribes() {
        assert_eq!(reach(0, 100), 0);
        assert_eq!(reach(9, 0), 0);
    }

    #[test]
    fn reach_never_goes_negative_or_overflows() {
        assert_eq!(reach(-1, 100), 0);
        assert_eq!(reach(5, -1), 0);
        assert_eq!(reach(i64::MAX, 2), i64::MAX);
    }

    #[test]
    fn first_step_is_visible_at_zero() {
        let progress = milestone_progress(0);
        assert_eq!(progress.current, None);
        assert_eq!(
            progress.next,
            Some(MilestoneTier {
                threshold: 1,
                name: "Contributor"
            })
        );
        assert!(close(progress.progress_to_next.unwrap(), 0.0));
        assert_eq!(progress.remaining_to_next, Some(1));
    }

    #[test]
    fn tier_is_reached_exactly_at_its_threshold() {
        let progress = milestone_progress(1);
        assert_eq!(
            progress.current,
            Some(MilestoneTier {
                threshold: 1,
                name: "Contributor"
            })
        );
        assert_eq!(progress.next.map(|tier| tier.threshold), Some(5));
        assert_eq!(progress.remaining_to_next, Some(4));
    }

    #[test]
    fn progress_is_measured_within_the_current_tier() {
        let progress = milestone_progress(30);
        assert_eq!(
            progress.current.map(|tier| tier.name),
            Some("Core Contributor")
        );
        assert_eq!(progress.next.map(|tier| tier.name), Some("Deck Steward"));
        assert!(close(progress.progress_to_next.unwrap(), 0.2));
        assert_eq!(progress.remaining_to_next, Some(20));
    }

    #[test]
    fn progress_reaches_one_just_before_the_next_tier() {
        let progress = milestone_progress(4);
        assert!(close(progress.progress_to_next.unwrap(), 0.75));
        assert_eq!(progress.remaining_to_next, Some(1));
    }

    #[test]
    fn top_tier_has_no_next_step() {
        let progress = milestone_progress(500);
        assert_eq!(progress.current.map(|tier| tier.name), Some("Cornerstone"));
        assert_eq!(progress.next, None);
        assert_eq!(progress.progress_to_next, None);
        assert_eq!(progress.remaining_to_next, None);
    }

    #[test]
    fn beyond_the_top_tier_stays_at_the_top_tier() {
        let progress = milestone_progress(100_000);
        assert_eq!(progress.current.map(|tier| tier.name), Some("Cornerstone"));
        assert_eq!(progress.next, None);
    }

    #[test]
    fn negative_accepted_is_treated_as_zero() {
        let progress = milestone_progress(-5);
        assert_eq!(progress.accepted, 0);
        assert_eq!(progress.current, None);
    }

    #[test]
    fn migration_tiers_are_strictly_increasing() {
        for pair in MILESTONES.windows(2) {
            assert!(
                pair[0].threshold < pair[1].threshold,
                "milestone thresholds must increase: {:?} then {:?}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn deck_name_uses_the_last_path_segment() {
        assert_eq!(
            deck_display_name(Some("Med::Anatomy::Upper Limb"), None),
            "Upper Limb"
        );
    }

    #[test]
    fn deck_name_falls_back_to_the_plain_name() {
        assert_eq!(deck_display_name(None, Some("Genetics")), "Genetics");
        assert_eq!(deck_display_name(Some(""), Some("Genetics")), "Genetics");
    }

    #[test]
    fn deck_name_falls_back_when_the_path_ends_in_a_separator() {
        assert_eq!(
            deck_display_name(Some("Med::"), Some("Genetics")),
            "Genetics"
        );
        assert_eq!(deck_display_name(Some("Med::"), None), "[Unnamed Deck]");
    }

    #[test]
    fn deck_name_defaults_when_nothing_is_usable() {
        assert_eq!(deck_display_name(None, None), "[Unnamed Deck]");
        assert_eq!(deck_display_name(Some("   "), Some("")), "[Unnamed Deck]");
    }

    #[test]
    fn days_since_counts_whole_days() {
        let then = chrono::DateTime::parse_from_rfc3339("2026-10-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let now = chrono::DateTime::parse_from_rfc3339("2026-10-08T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(days_since(then, now), 7);
    }

    #[test]
    fn days_since_is_zero_for_the_same_moment() {
        let now = chrono::Utc::now();
        assert_eq!(days_since(now, now), 0);
    }

    #[test]
    fn days_since_truncates_partial_days() {
        let then = chrono::DateTime::parse_from_rfc3339("2026-10-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let now = chrono::DateTime::parse_from_rfc3339("2026-10-07T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(days_since(then, now), 6);
    }

    #[test]
    fn days_since_never_goes_negative() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-10-08T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let future = chrono::DateTime::parse_from_rfc3339("2026-10-11T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(days_since(future, now), 0);
    }
}
