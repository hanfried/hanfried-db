use crate::datatypes::varcount::Varcount;
use crate::datatypes::HfdbSerializableDatatype;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct Page {
    byte_buffer: Arc<Mutex<Box<[u8]>>>,
}

impl Page {
    pub fn new(block_size: NonZeroUsize) -> Self {
        Self {
            // byte_buffer: vec![0; usize::from(block_size)].into(),
            byte_buffer: Arc::new(Mutex::new(vec![0; block_size.get()].into_boxed_slice())),
        }
    }

    pub fn from_vec(buffer: Vec<u8>) -> Self {
        Page {
            byte_buffer: Arc::new(Mutex::new(buffer.into_boxed_slice())),
        }
    }

    // pub fn get_i32(&self, offset: usize) -> i32 {
    //     i32::from_le_bytes(
    //         self.byte_buffer.lock().unwrap()[offset..offset + 4]
    //             .try_into()
    //             .unwrap(),
    //     )
    // }
    //
    // pub fn set_i32(&self, offset: usize, value: i32) {
    //     self.byte_buffer.lock().unwrap()[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    // }
    //
    pub fn get_bytes(&self, offset: usize) -> Vec<u8> {
        // let length = self.get_i32(offset);
        let length = self.get::<Varcount>(offset);
        let offset = offset + length.serialized_length();
        self.byte_buffer.lock().unwrap()[offset..offset + usize::from(&length)].to_vec()
    }

    pub fn set_bytes(&self, offset: usize, value: &[u8]) {
        // self.set_i32(offset, value.len() as i32);
        let length = Varcount::from(value.len());
        self.set(offset, &length);
        let offset = offset + length.serialized_length();
        self.byte_buffer.lock().unwrap()[offset..offset + value.len()].copy_from_slice(value);
    }

    // pub fn get_string(&self, offset: usize) -> String {
    //     let bytes = self.get_bytes(offset);
    //     String::from_utf8(bytes).unwrap()
    // }
    //
    // pub fn set_string(&mut self, offset: usize, value: &str) {
    //     self.set_bytes(offset, value.as_bytes());
    // }
    //
    // pub fn max_length(&self, s: &str) -> usize {
    //     4 + s.len()
    // }

    pub fn get_contents(&self) -> Vec<u8> {
        self.byte_buffer.lock().unwrap().to_vec()
    }

    pub fn set_contents(&self, value: &[u8]) {
        self.byte_buffer.lock().unwrap().copy_from_slice(value);
    }

    pub fn set<T: HfdbSerializableDatatype>(&self, offset: usize, value: &T) {
        value.serialize(&mut self.byte_buffer.lock().unwrap().as_mut()[offset..]);
    }

    pub fn get<T: HfdbSerializableDatatype>(&self, offset: usize) -> T {
        T::deserialize(self.byte_buffer.lock().unwrap()[offset..].as_ref())
    }
}
