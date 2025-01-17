#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct BlockId<'a> {
    pub filename: &'a str,
    pub block_number: usize,
}

#[allow(dead_code)]
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
