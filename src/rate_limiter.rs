use axum::http::StatusCode;
use chrono::{DateTime, Datelike, Utc};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

// Add tower-governor and governor imports

use crate::database::SharedConn;

// ======== Configuration Constants ========

const METRICS_REPORT_INTERVAL_SECS: u64 = 3600 * 24;

/// Storage Limits
const MAX_STORAGE_BYTES: u64 = 500 * 1024 * 1024 * 1024; // set in env
const MAX_USER_STORAGE_BYTES: u64 = 10000 * 1024 * 1024; // 10GB per user?

/// User-based limits
const MAX_USER_DAILY_UPLOADS: u32 = 100_000; // 20k uploads per day (for now the transition phase, will reduce later)
const MAX_USER_DAILY_DOWNLOADS: u32 = 100_000; // 50k downloads per day

/// Global limits
const MAX_MONTHLY_OPERATIONS: u64 = 100_000_000; // 100 million operations per month (for now the transition phase, will reduce later)

// ======== Data Structures ========

/// Tracking IP-based daily download/upload usage
#[derive(Clone, Debug)]
struct IpUsageCounter {
    downloads: u32,
    uploads: u32,
    reset_at: DateTime<Utc>,
}

/// Tracking user resource usage
#[derive(Clone, Debug)]
pub struct UserQuota {
    storage_used: Arc<AtomicU64>,
    upload_count: Arc<AtomicU32>,
    download_count: Arc<AtomicU32>,
    last_reset: Arc<Mutex<DateTime<Utc>>>,
}

/// Storage capacity monitoring
#[derive(Clone, Debug)]
pub struct StorageMonitor {
    enabled: Arc<AtomicBool>,
    current_bytes: Arc<AtomicU64>,
    max_bytes: u64,
    last_logged_percent: Arc<AtomicU64>,
}

type DbConn =
    Arc<bb8_postgres::bb8::Pool<bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>>>;

#[derive(Clone)]
pub struct RateLimiter {
    storage_monitor: Arc<StorageMonitor>,
    ip_usage_counters: Arc<Mutex<HashMap<IpAddr, IpUsageCounter>>>,
    operation_count: Arc<AtomicU64>,
    operation_reset_time: Arc<Mutex<DateTime<Utc>>>,
    user_quotas: Arc<Mutex<HashMap<i32, UserQuota>>>,
    db_conn: DbConn,
}

impl StorageMonitor {
    #[must_use]
    pub fn new(current_bucket_size: u64, max_bytes: u64) -> Self {
        // Get largest threshold that is already passed
        let thresholds = [10, 25, 50, 75, 80, 90, 95, 98];
        let curr_perc = current_bucket_size as f64 / max_bytes as f64 * 100.0;
        let enabled = curr_perc < 100.0;
        let mut last_passed = 0;
        for threshold in &thresholds {
            if curr_perc >= *threshold as f64 {
                last_passed = *threshold;
            }
        }

        let monitor = Self {
            enabled: Arc::new(AtomicBool::new(enabled)),
            current_bytes: Arc::new(AtomicU64::new(current_bucket_size)),
            max_bytes,
            last_logged_percent: Arc::new(AtomicU64::new(last_passed)),
        };

        println!("Storage monitor initialized with capacity of {max_bytes} bytes");
        monitor
    }

    /// Check if a storage operation of the given size is allowed
    #[must_use]
    pub fn check_operation(&self, bytes: u64) -> bool {
        if !self.enabled.load(Ordering::Relaxed) {
            println!("Storage monitor disabled - rejecting operation");
            return false;
        }

        let current = self.current_bytes.load(Ordering::Relaxed);
        let new_total = current + bytes;

        if new_total > self.max_bytes {
            // Disable further operations
            self.enabled.store(false, Ordering::Relaxed);
            println!(
                "Storage limit exceeded: {}/{} bytes - disabling operations",
                new_total, self.max_bytes
            );
            return false;
        }

        let percent_used = (new_total as f64 / self.max_bytes as f64 * 100.0) as u64;
        let last_logged = self.last_logged_percent.load(Ordering::Relaxed);

        let thresholds = [10, 25, 50, 75, 80, 90, 95, 98];
        for threshold in &thresholds {
            if percent_used >= *threshold && last_logged < *threshold {
                self.last_logged_percent
                    .store(*threshold, Ordering::Relaxed);
                println!("STORAGE ALERT: Usage at {threshold}%");
                break;
            }
        }

        true
    }

    /// Record a completed storage operation
    pub fn track_operation(&self, bytes: u64) {
        self.current_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Get current usage statistics
    #[must_use]
    pub fn get_usage(&self) -> (u64, u64, bool, f64) {
        let current = self.current_bytes.load(Ordering::Relaxed);
        let max = self.max_bytes;
        let enabled = self.enabled.load(Ordering::Relaxed);
        let percent = (current as f64 / max as f64) * 100.0;
        (current, max, enabled, percent)
    }
}

impl RateLimiter {
    /// Create a new rate limiter with default settings
    #[must_use]
    pub fn new(db: DbConn, current_bucket_size: u64) -> Self {
        let max_storage_bytes = std::env::var("MAX_STORAGE_BYTES")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(MAX_STORAGE_BYTES);

        let storage_monitor = Arc::new(StorageMonitor::new(current_bucket_size, max_storage_bytes));

        let limiter = Self {
            storage_monitor,
            ip_usage_counters: Arc::new(Mutex::new(HashMap::new())),
            operation_count: Arc::new(AtomicU64::new(0)),
            operation_reset_time: Arc::new(Mutex::new(Utc::now())),
            user_quotas: Arc::new(Mutex::new(HashMap::new())),
            db_conn: db,
        };

        limiter.start_background_tasks();

        limiter
    }

    // Add method to persist quotas to database
    pub async fn persist_user_quotas(
        &self,
        db_client: &mut SharedConn,
    ) -> Result<(), tokio_postgres::Error> {
        let quotas = self.user_quotas.lock().await;
        let now = Utc::now();

        let tx = db_client.transaction().await?;

        for (user_id, quota) in quotas.iter() {
            let storage_used = quota.storage_used.load(Ordering::Relaxed);
            let upload_count = quota.upload_count.load(Ordering::Relaxed);
            let download_count = quota.download_count.load(Ordering::Relaxed);

            tx.execute(
                "INSERT INTO user_quotas (user_id, storage_used, upload_count, download_count, last_reset) 
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (user_id) 
                 DO UPDATE SET 
                    storage_used = $2, 
                    upload_count = $3, 
                    download_count = $4,
                    last_reset = $5",
                &[user_id, &(storage_used as i64), &(upload_count as i32), &(download_count as i32), &now]
            ).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn load_user_quotas(&self) -> Result<(), tokio_postgres::Error> {
        let db_client = self
            .db_conn
            .get()
            .await
            .map_err(|err| {
                sentry::with_scope(
                    |scope| {
                        scope.set_fingerprint(Some(&["rate_limiter", "db_connection"]));
                        scope.set_tag("operation", "load_user_quotas");
                    },
                    || {
                        sentry::capture_message(
                            &format!("[load_user_quotas] Database connection failed: {}", err),
                            sentry::Level::Error,
                        );
                    },
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal Error".to_string(),
                )
            })
            .unwrap();

        let rows = db_client
            .query(
                "SELECT user_id, storage_used, upload_count, download_count, last_reset 
             FROM user_quotas",
                &[],
            )
            .await?;

        let mut quotas = self.user_quotas.lock().await;

        for row in rows {
            let user_id: i32 = row.get(0);
            let storage_used: i64 = row.get(1);
            let upload_count: i32 = row.get(2);
            let download_count: i32 = row.get(3);
            let last_reset: DateTime<Utc> = row.get(4);

            quotas.insert(
                user_id,
                UserQuota {
                    storage_used: Arc::new(AtomicU64::new(storage_used as u64)),
                    upload_count: Arc::new(AtomicU32::new(upload_count as u32)),
                    download_count: Arc::new(AtomicU32::new(download_count as u32)),
                    last_reset: Arc::new(Mutex::new(last_reset)),
                },
            );
        }

        println!("Loaded {} user quotas from database", quotas.len());
        Ok(())
    }

    fn start_background_tasks(&self) {
        let ip_usage_counters = self.ip_usage_counters.clone();
        let operation_count = self.operation_count.clone();
        let operation_reset_time = self.operation_reset_time.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600)); // Every hour
            loop {
                interval.tick().await;

                // Clean up expired IP usage counters
                let now = Utc::now();
                let mut counters = ip_usage_counters.lock().await;
                counters.retain(|_, counter| {
                    // Keep only if reset_at is today or in the future
                    now.date_naive() <= counter.reset_at.date_naive()
                });

                // Check if we need to reset monthly operation counter
                let mut reset_time = operation_reset_time.lock().await;
                if now.month() != reset_time.month() || now.year() != reset_time.year() {
                    operation_count.store(0, Ordering::Relaxed);
                    *reset_time = now;
                    println!("Monthly operation counter reset at {now}");
                }
            }
        });

        // Usage metrics reporting
        let storage_monitor = self.storage_monitor.clone();
        let operation_count = self.operation_count.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(METRICS_REPORT_INTERVAL_SECS));
            loop {
                interval.tick().await;
                let (current, max, enabled, percent) = storage_monitor.get_usage();
                let ops_count = operation_count.load(Ordering::Relaxed);

                println!(
                    "Storage metrics: {current}/{max} bytes ({percent:.1}%) - Enabled: {enabled}"
                );
                println!("Operations this month: {ops_count}/{MAX_MONTHLY_OPERATIONS}");
            }
        });

        // Periodic saving to db
        let rate_limiter = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(600)); // Every 10 minutes
            loop {
                interval.tick().await;

                match rate_limiter.db_conn.get_owned().await {
                    Ok(mut client) => {
                        if let Err(err) = rate_limiter.persist_user_quotas(&mut client).await {
                            sentry::with_scope(
                                |scope| {
                                    scope.set_fingerprint(Some(&["rate_limiter", "persist_quotas"]));
                                    scope.set_tag("operation", "persist_user_quotas");
                                },
                                || {
                                    sentry::capture_message(
                                        &format!("[persist_user_quotas] Failed to persist: {}", err),
                                        sentry::Level::Error,
                                    );
                                },
                            );
                        }
                    }
                    Err(err) => {
                        sentry::with_scope(
                            |scope| {
                                scope.set_fingerprint(Some(&["rate_limiter", "db_connection"]));
                                scope.set_tag("operation", "persist_user_quotas");
                            },
                            || {
                                sentry::capture_message(
                                    &format!("[persist_user_quotas] Database connection failed: {}", err),
                                    sentry::Level::Error,
                                );
                            },
                        );
                    }
                }
            }
        });
    }

    /// Get the storage monitor
    #[must_use]
    pub fn storage_monitor(&self) -> Arc<StorageMonitor> {
        self.storage_monitor.clone()
    }

    /// Check if an IP is allowed to download content
    async fn check_ip_download_allowed(&self, ip: IpAddr, file_count: u32) -> bool {
        // First check global operation limits
        if !self.check_global_operation() {
            return false;
        }

        // Then check IP-specific daily limits
        let mut counters = self.ip_usage_counters.lock().await;
        let now = Utc::now();

        // Get or create counter for this IP
        let counter = counters.entry(ip).or_insert_with(|| IpUsageCounter {
            downloads: 0,
            uploads: 0,
            reset_at: now,
        });

        // Check if we need to reset the counter (new day)
        if now.date_naive() > counter.reset_at.date_naive() {
            counter.downloads = 0;
            counter.uploads = 0;
            counter.reset_at = now;
        }

        // Check if download limit exceeded
        if counter.downloads + file_count >= MAX_USER_DAILY_DOWNLOADS {
            // println!("Download limit exceeded for IP {}: {}/{}",
            //      ip, counter.downloads, MAX_USER_DAILY_DOWNLOADS);
            return false;
        }

        true
    }

    /// Check if an IP is allowed to upload content
    async fn check_ip_upload_allowed(
        &self,
        ip: IpAddr,
        file_count: u32,
        content_bytes: u64,
    ) -> bool {
        // First check global operation and storage limits
        if !self.check_global_operation() || !self.storage_monitor.check_operation(content_bytes) {
            return false;
        }

        // Then check IP-specific daily limits
        let mut counters = self.ip_usage_counters.lock().await;
        let now = Utc::now();

        // Get or create counter for this IP
        let counter = counters.entry(ip).or_insert_with(|| IpUsageCounter {
            downloads: 0,
            uploads: 0,
            reset_at: now,
        });

        // Check if we need to reset the counter (new day)
        if now.date_naive() > counter.reset_at.date_naive() {
            counter.downloads = 0;
            counter.uploads = 0;
            counter.reset_at = now;
        }

        // safe threshhold as user bc they should be the same unles they are in a shared network idk
        if counter.uploads + file_count >= MAX_USER_DAILY_UPLOADS {
            // println!("Upload limit exceeded for IP {}: {}/{}",
            //      ip, counter.uploads, MAX_USER_DAILY_UPLOADS);
            return false;
        }

        true
    }

    // ======== Global Operation Management ========

    /// Check if global operation limits allow a new operation
    fn check_global_operation(&self) -> bool {
        let ops_count = self.operation_count.fetch_add(1, Ordering::Relaxed);
        if ops_count >= MAX_MONTHLY_OPERATIONS {
            println!("Monthly operation limit exceeded: {ops_count}/{MAX_MONTHLY_OPERATIONS}");
            return false;
        }
        true
    }

    // ======== User-based Quota Methods ========

    pub async fn check_user_upload_allowed(
        &self,
        user_id: i32,
        ip: IpAddr,
        file_count: u32,
        bytes: u64,
    ) -> bool {
        // also does global checks etc
        if !self.check_ip_upload_allowed(ip, file_count, bytes).await {
            return false;
        }

        let mut quotas = self.user_quotas.lock().await;
        let now = Utc::now();

        // Get or create user quota
        let quota = quotas.entry(user_id).or_insert_with(|| UserQuota {
            storage_used: Arc::new(AtomicU64::new(0)),
            upload_count: Arc::new(AtomicU32::new(0)),
            download_count: Arc::new(AtomicU32::new(0)),
            last_reset: Arc::new(Mutex::new(now)),
        });

        // Reset counters if it's a new day
        let mut last_reset = quota.last_reset.lock().await;
        if now.date_naive() > last_reset.date_naive() {
            quota.upload_count.store(0, Ordering::Relaxed);
            quota.download_count.store(0, Ordering::Relaxed);
            *last_reset = now;
        }

        // Check storage quota
        let current_storage = quota.storage_used.load(Ordering::Relaxed);
        if current_storage + bytes > MAX_USER_STORAGE_BYTES {
            println!(
                "User {} exceeded storage quota: {}/{} bytes",
                user_id,
                current_storage + bytes,
                MAX_USER_STORAGE_BYTES
            );
            return false;
        }

        // Check daily upload limit
        let uploads = quota.upload_count.load(Ordering::Relaxed);
        if uploads + file_count >= MAX_USER_DAILY_UPLOADS {
            // println!("User {} exceeded daily upload limit: {}/{}",
            //      user_id, uploads, MAX_USER_DAILY_UPLOADS);
            return false;
        }

        true
    }

    pub async fn check_user_download_allowed(
        &self,
        user_id: i32,
        ip: IpAddr,
        file_count: u32,
    ) -> bool {
        // also does global checks etc
        if !self.check_ip_download_allowed(ip, file_count).await {
            return false;
        }

        let mut quotas = self.user_quotas.lock().await;
        let now = Utc::now();

        // Get or create user quota
        let quota = quotas.entry(user_id).or_insert_with(|| UserQuota {
            storage_used: Arc::new(AtomicU64::new(0)),
            upload_count: Arc::new(AtomicU32::new(0)),
            download_count: Arc::new(AtomicU32::new(0)),
            last_reset: Arc::new(Mutex::new(now)),
        });

        // Reset counters if it's a new day
        let mut last_reset = quota.last_reset.lock().await;
        if now.date_naive() > last_reset.date_naive() {
            quota.upload_count.store(0, Ordering::Relaxed);
            quota.download_count.store(0, Ordering::Relaxed);
            *last_reset = now;
        }

        // Check daily download limit
        let downloads = quota.download_count.load(Ordering::Relaxed);
        if downloads + file_count >= MAX_USER_DAILY_DOWNLOADS {
            // println!("User {} exceeded daily download limit: {}/{}",
            //      user_id, downloads, MAX_USER_DAILY_DOWNLOADS);
            return false;
        }

        true
    }

    /// Track successful upload
    pub async fn track_user_upload(&self, user_id: i32, ip: IpAddr, bytes: u64, file_count: u32) {
        let quotas = self.user_quotas.lock().await;
        if let Some(quota) = quotas.get(&user_id) {
            // Update storage used
            quota.storage_used.fetch_add(bytes, Ordering::Relaxed);

            // Increment counters
            quota.upload_count.fetch_add(file_count, Ordering::Relaxed);
        }

        // add ip tracking
        let mut counters = self.ip_usage_counters.lock().await;
        if let Some(counter) = counters.get_mut(&ip) {
            counter.uploads += file_count;
        }

        // Note we don't track the "storage used globally" here, we do that in the confirmation AFTER the upload. The user metric storage_used is an estimate of attempted uploads
    }

    pub async fn track_user_download(&self, user_id: i32, ip: IpAddr, file_count: u32) {
        let quotas = self.user_quotas.lock().await;
        if let Some(quota) = quotas.get(&user_id) {
            // Increment download counter
            quota
                .download_count
                .fetch_add(file_count, Ordering::Relaxed);
        }

        // add ip tracking
        let mut counters = self.ip_usage_counters.lock().await;
        if let Some(counter) = counters.get_mut(&ip) {
            counter.downloads += file_count;
        }
    }

    /// Get user quota information
    pub async fn get_user_quota(&self, user_id: i32) -> Option<UserQuotaInfo> {
        let quotas = self.user_quotas.lock().await;
        if let Some(quota) = quotas.get(&user_id) {
            let storage_used = quota.storage_used.load(Ordering::Relaxed);
            let uploads_today = quota.upload_count.load(Ordering::Relaxed);
            let downloads_today = quota.download_count.load(Ordering::Relaxed);

            return Some(UserQuotaInfo {
                user_id,
                storage_used,
                storage_limit: MAX_USER_STORAGE_BYTES,
                storage_percent: (storage_used as f64 / MAX_USER_STORAGE_BYTES as f64) * 100.0,
                uploads_today,
                downloads_today,
            });
        }
        None
    }
}

/// User quota information for reporting
#[derive(Debug, Clone)]
pub struct UserQuotaInfo {
    pub user_id: i32,
    pub storage_used: u64,
    pub storage_limit: u64,
    pub storage_percent: f64,
    pub uploads_today: u32,
    pub downloads_today: u32,
}

// ======== Admin Endpoints ========

// Get storage and rate limiting status
// pub async fn get_storage_status(
//     State(rate_limiter): State<RateLimiter>,
// ) -> impl axum::response::IntoResponse {
//     use axum::Json;
//     use serde_json::json;

//     let (current, max, enabled, percent) = rate_limiter.storage_monitor().get_usage();
//     let (ops_count, ip_count, download_bytes, upload_bytes) = rate_limiter.get_overall_usage_stats().await;

//     let response = json!({
//         "storage": {
//             "current_bytes": current,
//             "max_bytes": max,
//             "percent_used": percent,
//             "enabled": enabled
//         },
//         "operations": {
//             "monthly_count": ops_count,
//             "monthly_limit": MAX_MONTHLY_OPERATIONS,
//             "percent_used": (ops_count as f64 / MAX_MONTHLY_OPERATIONS as f64) * 100.0
//         },
//         "usage": {
//             "unique_ips": ip_count,
//             "total_bytes_downloaded": download_bytes,
//             "total_bytes_uploaded": upload_bytes
//         }
//     });

//     (StatusCode::OK, Json(response))
// }
