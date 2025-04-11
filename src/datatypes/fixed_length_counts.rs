use crate::datatypes::HfdbSerializableDatatype;

#[derive(Debug, Eq, PartialEq)]
pub struct TinyCount(u8);
impl HfdbSerializableDatatype for TinyCount {
    fn serialized_length(&self) -> usize {
        1
    }

    fn serialize(&self, buffer: &mut [u8]) {
        buffer[..1].copy_from_slice(&self.0.to_le_bytes());
    }

    fn deserialize(buffer: &[u8]) -> Self {
        let buffer = [buffer[0]; 1];
        Self(u8::from_le_bytes(buffer))
    }
}

impl From<u8> for TinyCount {
    fn from(value: u8) -> Self {
        Self(value)
    }
}

impl From<TinyCount> for u8 {
    fn from(value: TinyCount) -> u8 {
        value.0
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct SmallCount(u16);
impl HfdbSerializableDatatype for SmallCount {
    fn serialized_length(&self) -> usize {
        2
    }

    fn serialize(&self, buffer: &mut [u8]) {
        buffer[..2].copy_from_slice(&self.0.to_le_bytes());
    }

    fn deserialize(buffer: &[u8]) -> Self {
        let buffer = [buffer[0], buffer[1]];
        Self(u16::from_le_bytes(buffer))
    }
}

impl From<u16> for SmallCount {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

impl From<SmallCount> for u16 {
    fn from(value: SmallCount) -> u16 {
        value.0
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Count(u32);
impl HfdbSerializableDatatype for Count {
    fn serialized_length(&self) -> usize {
        4
    }

    fn serialize(&self, buffer: &mut [u8]) {
        buffer[..4].copy_from_slice(&self.0.to_le_bytes());
    }

    fn deserialize(buffer: &[u8]) -> Self {
        let buffer = [buffer[0], buffer[1], buffer[2], buffer[3]];
        Self(u32::from_le_bytes(buffer))
    }
}

impl From<u32> for Count {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<Count> for u32 {
    fn from(value: Count) -> u32 {
        value.0
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct BigCount(u64);
impl HfdbSerializableDatatype for BigCount {
    fn serialized_length(&self) -> usize {
        8
    }

    fn serialize(&self, buffer: &mut [u8]) {
        buffer[..8].copy_from_slice(&self.0.to_le_bytes());
    }

    fn deserialize(buffer: &[u8]) -> Self {
        let mut _buffer = [0u8; 8];
        _buffer.copy_from_slice(&buffer[..8]);
        Self(u64::from_le_bytes(_buffer))
    }
}

impl From<u64> for BigCount {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<BigCount> for u64 {
    fn from(value: BigCount) -> u64 {
        value.0
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct HugeCount(u128);
impl HfdbSerializableDatatype for HugeCount {
    fn serialized_length(&self) -> usize {
        16
    }

    fn serialize(&self, buffer: &mut [u8]) {
        buffer[..16].copy_from_slice(&self.0.to_le_bytes());
    }

    fn deserialize(buffer: &[u8]) -> Self {
        let mut _buffer = [0u8; 16];
        _buffer.copy_from_slice(&buffer[..16]);
        Self(u128::from_le_bytes(_buffer))
    }
}

impl From<u128> for HugeCount {
    fn from(value: u128) -> Self {
        Self(value)
    }
}

impl From<HugeCount> for u128 {
    fn from(value: HugeCount) -> u128 {
        value.0
    }
}
