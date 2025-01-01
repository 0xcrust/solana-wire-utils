use std::time::Duration;

use anyhow::anyhow;
use log::{debug, error};

pub fn spawn_retry_task<F, G, T, E>(
    task_fn: G,
    task_name: String,
    max_attempts: usize,
    delay_ms: u64,
) -> tokio::task::JoinHandle<anyhow::Result<T>>
where
    F: std::future::Future<Output = Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Display + Send + 'static,
    G: Fn() -> F + Send + Sync + 'static,
{
    tokio::spawn(async move {
        let mut attempts = 0;
        loop {
            match task_fn().await {
                Ok(ret) => {
                    debug!("task {} succeeded", task_name);
                    return Ok(ret);
                }
                Err(e) => {
                    if attempts > max_attempts {
                        error!(
                            "task {} failed with max-attempts {}: {}",
                            task_name, max_attempts, e
                        );
                        return Err(anyhow!("{}", e));
                    } else {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                }
            }
            attempts += 1;
        }
    })
}
