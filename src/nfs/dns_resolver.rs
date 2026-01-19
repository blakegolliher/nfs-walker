//! DNS-aware load balancing for NFS connections
//!
//! This module provides a `DnsResolver` that:
//! - Resolves hostnames to all available IP addresses at startup
//! - Assigns IPs to workers using round-robin distribution
//! - Tracks health per IP (failures trigger failover)
//! - Periodically refreshes DNS to discover new IPs
//! - Provides sticky IP assignments with failover on RPC errors

use crate::nfs::resolve_dns;
use std::sync::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Number of consecutive failures before marking an IP as unhealthy
const FAILURE_THRESHOLD: u32 = 3;

/// Cooldown period before retrying an unhealthy IP (seconds)
const UNHEALTHY_COOLDOWN_SECS: u64 = 30;

/// Default DNS refresh interval (seconds)
pub const DEFAULT_DNS_REFRESH_SECS: u64 = 60;

/// Health status of an IP address
#[derive(Debug, Clone)]
struct IpHealth {
    /// Consecutive failure count
    failures: u32,
    /// Time when the IP became unhealthy (for cooldown)
    unhealthy_since: Option<Instant>,
}

impl Default for IpHealth {
    fn default() -> Self {
        Self {
            failures: 0,
            unhealthy_since: None,
        }
    }
}

impl IpHealth {
    fn is_healthy(&self) -> bool {
        if self.failures < FAILURE_THRESHOLD {
            return true;
        }
        // Check if cooldown has elapsed
        if let Some(since) = self.unhealthy_since {
            since.elapsed() >= Duration::from_secs(UNHEALTHY_COOLDOWN_SECS)
        } else {
            true
        }
    }

    fn record_failure(&mut self) {
        self.failures += 1;
        if self.failures >= FAILURE_THRESHOLD && self.unhealthy_since.is_none() {
            self.unhealthy_since = Some(Instant::now());
        }
    }

    fn record_success(&mut self) {
        self.failures = 0;
        self.unhealthy_since = None;
    }
}

/// Internal state protected by RwLock
struct ResolverState {
    /// All known IP addresses
    ips: Vec<String>,
    /// Health tracking per IP
    health: HashMap<String, IpHealth>,
    /// Worker -> assigned IP mapping (sticky assignment)
    assignments: HashMap<usize, String>,
}

impl ResolverState {
    fn new(ips: Vec<String>) -> Self {
        let health = ips.iter().map(|ip| (ip.clone(), IpHealth::default())).collect();
        Self {
            ips,
            health,
            assignments: HashMap::new(),
        }
    }

    /// Get healthy IPs
    fn healthy_ips(&self) -> Vec<&String> {
        self.ips
            .iter()
            .filter(|ip| {
                self.health
                    .get(*ip)
                    .map(|h| h.is_healthy())
                    .unwrap_or(true)
            })
            .collect()
    }

    /// Get or assign an IP for a worker
    fn get_or_assign_ip(&mut self, worker_id: usize) -> Option<String> {
        // Check if worker has an existing assignment
        if let Some(ip) = self.assignments.get(&worker_id) {
            // Verify the IP is still healthy
            if let Some(health) = self.health.get(ip) {
                if health.is_healthy() {
                    return Some(ip.clone());
                }
            }
            // IP is unhealthy, clear assignment
            self.assignments.remove(&worker_id);
        }

        // Assign new IP using round-robin among healthy IPs
        let healthy: Vec<_> = self.healthy_ips();
        if healthy.is_empty() {
            // Fallback: if all IPs are unhealthy, use any IP (best effort)
            if !self.ips.is_empty() {
                let ip = self.ips[worker_id % self.ips.len()].clone();
                self.assignments.insert(worker_id, ip.clone());
                return Some(ip);
            }
            return None;
        }

        let ip = healthy[worker_id % healthy.len()].clone();
        self.assignments.insert(worker_id, ip.clone());
        Some(ip)
    }

    /// Clear assignment for a worker (called on failure)
    fn clear_assignment(&mut self, worker_id: usize) {
        self.assignments.remove(&worker_id);
    }

    /// Update IPs from DNS refresh
    fn update_ips(&mut self, new_ips: Vec<String>) {
        let new_set: HashSet<_> = new_ips.iter().cloned().collect();
        let old_set: HashSet<_> = self.ips.iter().cloned().collect();

        // Add health entries for new IPs
        for ip in new_ips.iter() {
            if !old_set.contains(ip) {
                info!(ip = %ip, "DNS refresh discovered new IP");
                self.health.insert(ip.clone(), IpHealth::default());
            }
        }

        // Keep health for existing IPs, even if they disappear from DNS
        // (they might come back, and we want to remember their health)

        // Update the IP list
        self.ips = new_ips;

        // Clear assignments for IPs that no longer exist
        let to_remove: Vec<_> = self
            .assignments
            .iter()
            .filter(|(_, ip)| !new_set.contains(*ip))
            .map(|(worker_id, _)| *worker_id)
            .collect();

        for worker_id in to_remove {
            debug!(worker_id = worker_id, "Clearing assignment for removed IP");
            self.assignments.remove(&worker_id);
        }
    }
}

/// DNS-aware load balancer for NFS connections
///
/// Distributes workers across all IPs returned by DNS resolution,
/// with health tracking and automatic failover.
pub struct DnsResolver {
    /// Hostname being resolved
    hostname: String,
    /// Internal state
    state: RwLock<ResolverState>,
    /// DNS refresh interval (0 = disabled)
    #[allow(dead_code)]
    refresh_interval: Duration,
    /// Shutdown flag for background thread
    shutdown: Arc<AtomicBool>,
    /// Whether DNS load balancing is enabled
    enabled: bool,
}

impl DnsResolver {
    /// Create a new DNS resolver
    ///
    /// # Arguments
    /// * `hostname` - The NFS server hostname to resolve
    /// * `refresh_secs` - Seconds between DNS refreshes (0 = disabled)
    /// * `enabled` - Whether DNS load balancing is enabled
    pub fn new(hostname: &str, refresh_secs: u64, enabled: bool) -> Arc<Self> {
        let ips = if enabled {
            let resolved = resolve_dns(hostname);
            if resolved.is_empty() {
                warn!(hostname = %hostname, "DNS resolution returned no IPs, using hostname directly");
                vec![hostname.to_string()]
            } else {
                info!(
                    hostname = %hostname,
                    ips = ?resolved,
                    count = resolved.len(),
                    "DNS resolution complete"
                );
                resolved
            }
        } else {
            // DNS LB disabled - use hostname directly
            vec![hostname.to_string()]
        };

        let resolver = Arc::new(Self {
            hostname: hostname.to_string(),
            state: RwLock::new(ResolverState::new(ips)),
            refresh_interval: Duration::from_secs(refresh_secs),
            shutdown: Arc::new(AtomicBool::new(false)),
            enabled,
        });

        // Start background refresh thread if enabled
        if enabled && refresh_secs > 0 {
            let resolver_weak = Arc::downgrade(&resolver);
            let shutdown = Arc::clone(&resolver.shutdown);
            let hostname = hostname.to_string();
            let interval = Duration::from_secs(refresh_secs);

            thread::Builder::new()
                .name("dns-refresh".to_string())
                .spawn(move || {
                    dns_refresh_loop(resolver_weak, shutdown, hostname, interval);
                })
                .expect("Failed to spawn DNS refresh thread");

            debug!(
                hostname = %resolver.hostname,
                interval_secs = refresh_secs,
                "Started DNS refresh background thread"
            );
        }

        resolver
    }

    /// Create a resolver with DNS load balancing disabled
    pub fn disabled(hostname: &str) -> Arc<Self> {
        Self::new(hostname, 0, false)
    }

    /// Get all currently known IPs
    pub fn all_ips(&self) -> Vec<String> {
        self.state.read().unwrap().ips.clone()
    }

    /// Get count of healthy IPs
    pub fn healthy_count(&self) -> usize {
        self.state.read().unwrap().healthy_ips().len()
    }

    /// Get an IP for a worker (sticky assignment with failover)
    ///
    /// Returns the same IP for the same worker_id as long as it's healthy.
    /// If the assigned IP becomes unhealthy, a new one is assigned.
    pub fn get_ip_for_worker(&self, worker_id: usize) -> Option<String> {
        self.state.write().unwrap().get_or_assign_ip(worker_id)
    }

    /// Report a successful RPC call to an IP
    ///
    /// Resets the failure counter for this IP.
    pub fn report_success(&self, ip: &str) {
        let mut state = self.state.write().unwrap();
        if let Some(health) = state.health.get_mut(ip) {
            health.record_success();
        }
    }

    /// Report a failed RPC call to an IP
    ///
    /// Increments the failure counter. After FAILURE_THRESHOLD consecutive
    /// failures, the IP is marked unhealthy and workers are reassigned.
    pub fn report_failure(&self, ip: &str, worker_id: usize) {
        let mut state = self.state.write().unwrap();

        if let Some(health) = state.health.get_mut(ip) {
            health.record_failure();

            if health.failures >= FAILURE_THRESHOLD {
                warn!(
                    ip = %ip,
                    failures = health.failures,
                    "IP marked unhealthy after consecutive failures"
                );
                // Clear this worker's assignment so they get a new IP
                state.clear_assignment(worker_id);
            }
        }
    }

    /// Check if DNS load balancing is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Manually trigger a DNS refresh
    pub fn refresh(&self) {
        if !self.enabled {
            return;
        }

        let new_ips = resolve_dns(&self.hostname);
        if !new_ips.is_empty() {
            let mut state = self.state.write().unwrap();
            let old_count = state.ips.len();
            state.update_ips(new_ips);
            let new_count = state.ips.len();

            if old_count != new_count {
                info!(
                    hostname = %self.hostname,
                    old_count = old_count,
                    new_count = new_count,
                    "DNS refresh updated IP pool"
                );
            }
        }
    }
}

impl Drop for DnsResolver {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}

/// Background thread for periodic DNS refresh
fn dns_refresh_loop(
    resolver: std::sync::Weak<DnsResolver>,
    shutdown: Arc<AtomicBool>,
    hostname: String,
    interval: Duration,
) {
    debug!(hostname = %hostname, "DNS refresh thread started");

    loop {
        // Sleep for the interval
        thread::sleep(interval);

        // Check shutdown
        if shutdown.load(Ordering::Relaxed) {
            debug!(hostname = %hostname, "DNS refresh thread shutting down");
            break;
        }

        // Try to upgrade weak reference
        let resolver = match resolver.upgrade() {
            Some(r) => r,
            None => {
                debug!(hostname = %hostname, "DnsResolver dropped, refresh thread exiting");
                break;
            }
        };

        // Perform refresh
        let new_ips = resolve_dns(&hostname);
        if !new_ips.is_empty() {
            let mut state = resolver.state.write().unwrap();
            state.update_ips(new_ips);
            debug!(
                hostname = %hostname,
                ips = ?state.ips,
                "DNS refresh completed"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ip_health_default_is_healthy() {
        let health = IpHealth::default();
        assert!(health.is_healthy());
        assert_eq!(health.failures, 0);
    }

    #[test]
    fn test_ip_health_becomes_unhealthy() {
        let mut health = IpHealth::default();
        for _ in 0..FAILURE_THRESHOLD {
            health.record_failure();
        }
        assert!(!health.is_healthy());
    }

    #[test]
    fn test_ip_health_success_resets() {
        let mut health = IpHealth::default();
        health.record_failure();
        health.record_failure();
        assert!(health.is_healthy());
        health.record_success();
        assert_eq!(health.failures, 0);
    }

    #[test]
    fn test_resolver_state_round_robin() {
        let mut state = ResolverState::new(vec![
            "1.1.1.1".to_string(),
            "2.2.2.2".to_string(),
            "3.3.3.3".to_string(),
        ]);

        // Workers should get IPs in round-robin fashion
        let ip0 = state.get_or_assign_ip(0).unwrap();
        let ip1 = state.get_or_assign_ip(1).unwrap();
        let ip2 = state.get_or_assign_ip(2).unwrap();
        let ip3 = state.get_or_assign_ip(3).unwrap();

        assert_eq!(ip0, "1.1.1.1");
        assert_eq!(ip1, "2.2.2.2");
        assert_eq!(ip2, "3.3.3.3");
        assert_eq!(ip3, "1.1.1.1"); // Wraps around
    }

    #[test]
    fn test_resolver_state_sticky_assignment() {
        let mut state = ResolverState::new(vec![
            "1.1.1.1".to_string(),
            "2.2.2.2".to_string(),
        ]);

        let ip1 = state.get_or_assign_ip(0).unwrap();
        let ip2 = state.get_or_assign_ip(0).unwrap();

        assert_eq!(ip1, ip2); // Same worker gets same IP
    }

    #[test]
    fn test_resolver_state_failover() {
        let mut state = ResolverState::new(vec![
            "1.1.1.1".to_string(),
            "2.2.2.2".to_string(),
        ]);

        // Assign worker 0 to first IP
        let ip = state.get_or_assign_ip(0).unwrap();
        assert_eq!(ip, "1.1.1.1");

        // Mark that IP as unhealthy
        for _ in 0..FAILURE_THRESHOLD {
            state.health.get_mut("1.1.1.1").unwrap().record_failure();
        }
        state.clear_assignment(0);

        // Worker should now get a different IP
        let new_ip = state.get_or_assign_ip(0).unwrap();
        assert_eq!(new_ip, "2.2.2.2");
    }

    #[test]
    fn test_disabled_resolver() {
        let resolver = DnsResolver::disabled("test.example.com");
        assert!(!resolver.is_enabled());

        let ip = resolver.get_ip_for_worker(0).unwrap();
        assert_eq!(ip, "test.example.com"); // Returns hostname as-is
    }
}
