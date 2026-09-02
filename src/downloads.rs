use std::{collections::HashMap, path::PathBuf, sync::Arc, usize};

use tokio::sync::Mutex;

pub struct TorrentList {
    list: Mutex<HashMap<usize, PathBuf>>,
}

impl TorrentList {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            list: Mutex::new(HashMap::new()),
        })
    }
    
    pub async fn insert(&self, id: usize, path: PathBuf) {
        self.list.lock().await.insert(id, path);
    }
}
