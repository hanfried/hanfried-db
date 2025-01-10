#[derive(Debug)]
pub struct Page {
    byte_buffer: Vec<u8>,
}

impl Page {
    pub fn new(block_size: usize) -> Self {
        Self {
            byte_buffer: vec![0; block_size],
        }
    }

    pub fn get_i32(&self, offset: usize) -> i32 {
        i32::from_le_bytes(self.byte_buffer[offset..offset + 4].try_into().unwrap())
    }

    pub fn set_i32(&mut self, offset: usize, value: i32) {
        self.byte_buffer[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    pub fn get_bytes(&self, offset: usize) -> &[u8] {
        let length = self.get_i32(offset);
        &self.byte_buffer[offset + 4..offset + 4 + length as usize]
    }

    pub fn set_bytes(&mut self, offset: usize, value: &[u8]) {
        self.set_i32(offset, value.len() as i32);
        self.byte_buffer[offset + 4..offset + 4 + value.len()].copy_from_slice(value);
    }

    pub fn get_string(&self, offset: usize) -> &str {
        let bytes = self.get_bytes(offset);
        std::str::from_utf8(bytes).unwrap()
    }

    pub fn set_string(&mut self, offset: usize, value: &str) {
        self.set_bytes(offset, value.as_bytes());
    }

    pub fn max_length(&self, strlen: usize) -> usize {
        4 + strlen
    }

    pub fn get_contents(&self) -> &[u8] {
        self.byte_buffer.as_slice()
    }

    pub fn set_contents(&mut self, value: &[u8]) {
        self.byte_buffer.copy_from_slice(value);
    }
}
