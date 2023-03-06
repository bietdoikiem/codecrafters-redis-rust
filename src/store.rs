use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;

pub struct Entry {
    value: String,
    expiry: Option<Instant>,
}

pub struct Store {
    data: HashMap<String, Entry>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            data: HashMap::new()
        }
    }

    pub fn set(&mut self, key: String, value: String) {
        let entry = Entry {
            value,
            expiry: None
        };
        self.data.insert(key, entry);
    }

    pub fn set_px(&mut self, key: String, value: String, px: u64) {
        let entry = Entry {
            value,
            expiry: Some(Instant::now() + Duration::from_millis(px)),
        };
        // TODO: Implement lazy deletion expired key
        self.data.insert(key, entry);
    }

    pub fn get(&mut self, key: String) -> Option<String> {
        match self.data.get(key.as_str()) {
            Some(entry) => {
                if let Some(expiry) = &entry.expiry {
                    if Instant::now() > expiry.clone() {
                        self.data.remove(key.as_str());
                        return None;
                    }
                }

                Some(entry.value.clone())
            }
            None => None
        }
    }
}