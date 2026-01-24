use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::time::{self, Interval, MissedTickBehavior};

/// Shared throttle for all S3 calls to respect provider-wide RPS and concurrency limits.
#[derive(Clone, Debug)]
pub struct S3Throttle {
    permits: Arc<Semaphore>,
    interval: Arc<Mutex<Interval>>,
}

impl S3Throttle {
    /// Create a new throttle with the given max concurrency and requests-per-second limit.
    /// A small per-request wait is enforced via the interval, and a semaphore caps burstiness.
    #[must_use]
    pub fn new(max_concurrency: usize, max_rps: u32) -> Self {
        let spacing = if max_rps == 0 {
            Duration::from_secs(0)
        } else {
            Duration::from_secs_f64(1.0 / max_rps as f64)
        };

        let mut interval = time::interval(spacing.max(Duration::from_millis(1)));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        Self {
            permits: Arc::new(Semaphore::new(max_concurrency.max(1))),
            interval: Arc::new(Mutex::new(interval)),
        }
    }

    /// Wait for both rate and concurrency slots. Hold the returned permit for the duration
    /// of the S3 call to keep concurrency bounded.
    pub async fn acquire(&self) -> OwnedSemaphorePermit {
        {
            let mut interval = self.interval.lock().await;
            interval.tick().await;
        }

        self.permits
            .clone()
            .acquire_owned()
            .await
            .expect("S3 throttle semaphore closed")
    }
}
