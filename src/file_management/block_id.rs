#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct BlockId<'filenames> {
    pub filename: &'filenames str,
    pub block_number: usize,
}

impl BlockId<'_> {
    pub fn new(filename: &str, block_number: usize) -> BlockId {
        BlockId {
            filename,
            block_number,
        }
    }
}

// impl <'a> BlockId<'a> {
//
// }
