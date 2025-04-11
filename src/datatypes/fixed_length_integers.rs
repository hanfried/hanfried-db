use crate::datatypes::HfdbSerializableDatatype;

#[derive(Debug, Eq, PartialEq)]
pub struct TinyInteger(i8);
impl HfdbSerializableDatatype for TinyInteger {
    fn serialized_length(&self) -> usize {
        1
    }

    fn serialize(&self, buffer: &mut [u8]) {
        buffer[..1].copy_from_slice(&self.0.to_le_bytes());
    }

    fn deserialize(buffer: &[u8]) -> Self {
        let buffer = [buffer[0]; 1];
        Self(i8::from_le_bytes(buffer))
    }
}

impl From<i8> for TinyInteger {
    fn from(value: i8) -> Self {
        Self(value)
    }
}

impl From<TinyInteger> for i8 {
    fn from(value: TinyInteger) -> i8 {
        value.0
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct SmallInteger(i16);
impl HfdbSerializableDatatype for SmallInteger {
    fn serialized_length(&self) -> usize {
        2
    }

    fn serialize(&self, buffer: &mut [u8]) {
        buffer[..2].copy_from_slice(&self.0.to_le_bytes());
    }

    fn deserialize(buffer: &[u8]) -> Self {
        let buffer = [buffer[0], buffer[1]];
        Self(i16::from_le_bytes(buffer))
    }
}

impl From<i16> for SmallInteger {
    fn from(value: i16) -> Self {
        Self(value)
    }
}

impl From<SmallInteger> for i16 {
    fn from(value: SmallInteger) -> i16 {
        value.0
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Integer(i32);
impl HfdbSerializableDatatype for Integer {
    fn serialized_length(&self) -> usize {
        4
    }

    fn serialize(&self, buffer: &mut [u8]) {
        buffer[..4].copy_from_slice(&self.0.to_le_bytes());
    }

    fn deserialize(buffer: &[u8]) -> Self {
        let buffer = [buffer[0], buffer[1], buffer[2], buffer[3]];
        Self(i32::from_le_bytes(buffer))
    }
}

impl From<i32> for Integer {
    fn from(value: i32) -> Self {
        Self(value)
    }
}

impl From<Integer> for i32 {
    fn from(value: Integer) -> i32 {
        value.0
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct BigInteger(i64);
impl HfdbSerializableDatatype for BigInteger {
    fn serialized_length(&self) -> usize {
        8
    }

    fn serialize(&self, buffer: &mut [u8]) {
        buffer[..8].copy_from_slice(&self.0.to_le_bytes());
    }

    fn deserialize(buffer: &[u8]) -> Self {
        let mut _buffer = [0u8; 8];
        _buffer.copy_from_slice(&buffer[..8]);
        Self(i64::from_le_bytes(_buffer))
    }
}

impl From<i64> for BigInteger {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<BigInteger> for i64 {
    fn from(value: BigInteger) -> i64 {
        value.0
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct HugeInteger(i128);
impl HfdbSerializableDatatype for HugeInteger {
    fn serialized_length(&self) -> usize {
        16
    }

    fn serialize(&self, buffer: &mut [u8]) {
        buffer[..16].copy_from_slice(&self.0.to_le_bytes());
    }

    fn deserialize(buffer: &[u8]) -> Self {
        let mut _buffer = [0u8; 16];
        _buffer.copy_from_slice(&buffer[..16]);
        Self(i128::from_le_bytes(_buffer))
    }
}

impl From<i128> for HugeInteger {
    fn from(value: i128) -> Self {
        Self(value)
    }
}

impl From<HugeInteger> for i128 {
    fn from(value: HugeInteger) -> i128 {
        value.0
    }
}
