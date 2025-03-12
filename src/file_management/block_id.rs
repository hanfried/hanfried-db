#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockId {
    pub filename: String,
    pub block_number: usize,
}

impl BlockId {
    pub fn new(filename: &str, block_number: usize) -> BlockId {
        BlockId {
            filename: filename.to_string(),
            block_number,
        }
    }
}
