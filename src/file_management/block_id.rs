use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbFilename(Arc<String>);

// Todo: Probably put it into a BTreeMap or ResourceSyncCache with a number to it
// to avoid unnecessary having copies of it by accident ...

impl DbFilename {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Display for DbFilename {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for DbFilename {
    fn from(value: &str) -> Self {
        DbFilename(Arc::new(value.to_string()))
    }
}

impl From<String> for DbFilename {
    fn from(value: String) -> Self {
        DbFilename(Arc::new(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockId {
    filename: DbFilename,
    block_number: usize,
}

impl BlockId {
    pub fn new(filename: DbFilename, block_number: usize) -> Self {
        BlockId {
            filename,
            block_number,
        }
    }

    pub fn with_other_block_number(&self, block_number: usize) -> Self {
        BlockId {
            filename: self.filename.clone(),
            block_number,
        }
    }

    pub fn block_number(&self) -> usize {
        self.block_number
    }

    pub fn filename(&self) -> &DbFilename {
        &self.filename
    }
}
