use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use once_cell::sync::Lazy;
use std::sync::Mutex;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AgentTemplate {
    pub uri: String,
    pub default_channels: Vec<String>,
    pub default_properties: HashMap<String, String>,
}

pub struct AgentTemplateRegistry {
    templates: HashMap<String, AgentTemplate>,
}

impl AgentTemplateRegistry {
    pub fn new() -> Self {
        Self {
            templates: HashMap::new(),
        }
    }

    pub fn add_template(&mut self, template: AgentTemplate) {
        self.templates.insert(template.uri.clone(), template);
    }

    pub fn get_template(&self, uri: &str) -> Option<&AgentTemplate> {
        self.templates.get(uri)
    }

    pub fn initialize_default_templates(&mut self) {
        // Add default templates here
        let default_template = AgentTemplate {
            uri: "std/chatbot/1.0.0".to_string(),
            default_channels: vec!["input_output".to_string()],
            default_properties: HashMap::from([
                ("property1".to_string(), "default_value1".to_string()),
                ("property2".to_string(), "default_value2".to_string()),
            ]),
        };

        self.add_template(default_template);
    }
}

// Global instance of the AgentTemplateRegistry
static TEMPLATE_REGISTRY: Lazy<Mutex<AgentTemplateRegistry>> = Lazy::new(|| {
    let mut registry = AgentTemplateRegistry::new();
    registry.initialize_default_templates(); // Initialize with default templates
    Mutex::new(registry)
});

// Method to retrieve a template by URI
pub fn get_template_by_uri(uri: &str) -> Option<AgentTemplate> {
    let registry = TEMPLATE_REGISTRY.lock().unwrap();
    registry.get_template(uri).cloned() // Return a cloned template
}
