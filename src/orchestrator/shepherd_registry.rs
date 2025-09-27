use std::sync::Arc;

use dashmap::DashMap;

use crate::orchestrator::db::DomainsConfig;
use crate::orchestrator::event_stream::EventStream;
use crate::orchestrator::shepherd_manager::ShepherdManager;
use crate::orchestrator::taskset::TaskSetRegistry;

/// Registry that provides one ShepherdManager per domain
#[derive(Clone)]
pub struct ShepherdManagerRegistry {
    managers: Arc<DashMap<String, Arc<ShepherdManager>>>,
    domains_config: Arc<DomainsConfig>,
    task_registry: Arc<TaskSetRegistry>,
    event_stream: Arc<EventStream>,
}

impl ShepherdManagerRegistry {
    pub fn new(
        domains_config: Arc<DomainsConfig>,
        task_registry: Arc<TaskSetRegistry>,
        event_stream: Arc<EventStream>,
    ) -> Self {
        let registry = Self {
            managers: Arc::new(DashMap::new()),
            domains_config: domains_config.clone(),
            task_registry,
            event_stream,
        };

        // Eagerly create managers for configured domains (can add more on the fly)
        for domain in domains_config.specific.keys() {
            let _ = registry.get_or_create_manager(domain);
        }

        registry
    }

    pub fn get_or_create_manager(&self, domain: &str) -> Arc<ShepherdManager> {
        if let Some(mgr) = self.managers.get(domain) {
            mgr.clone()
        } else {
            let manager = Arc::new(ShepherdManager::new(
                self.domains_config.clone(),
                self.task_registry.clone(),
                self.event_stream.clone(),
            ));
            self.managers.insert(domain.to_string(), manager.clone());
            manager
        }
    }

    pub async fn shutdown_all(&self) {
        let managers: Vec<Arc<ShepherdManager>> = self
            .managers
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        for mgr in managers {
            let _ = mgr.shutdown().await;
        }
    }

    /// Find the domain manager that has the given shepherd UUID registered
    pub fn list_managers(&self) -> Vec<Arc<ShepherdManager>> {
        self.managers.iter().map(|e| e.value().clone()).collect()
    }
}
