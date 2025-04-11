use crate::datatypes::HfdbSerializableDatatype;

#[derive(Debug, Eq, PartialEq)]
pub struct Varlength(u64);

impl HfdbSerializableDatatype for Varlength {
    fn serialized_length(&self) -> usize {
        for leading_bits in 1..=8 {
            if self.0 < (1 << (leading_bits * 7)) {
                return leading_bits;
            }
        }
        9
    }

    fn serialize(&self, buffer: &mut [u8]) {
        let bytes_full = self.0.to_be_bytes();
        let length = self.serialized_length();
        if length <= 8 {
            buffer[..length].copy_from_slice(&bytes_full[8 - length..]);
            match length {
                2 => buffer[0] |= 0b1000_0000,
                3 => buffer[0] |= 0b1100_0000,
                4 => buffer[0] |= 0b1110_0000,
                5 => buffer[0] |= 0b1111_0000,
                6 => buffer[0] |= 0b1111_1000,
                7 => buffer[0] |= 0b1111_1100,
                8 => buffer[0] |= 0b1111_1110,
                _ => {}
            }
        } else {
            buffer[0] = 0b1111_1111;
            buffer[1..].copy_from_slice(&bytes_full);
        }
    }

    fn deserialize(buffer: &[u8]) -> Self {
        let mut bytes_full = [0u8; 8];
        match buffer[0] {
            0b0000_0000..0b1000_0000 => bytes_full[7] = buffer[0],
            0b1000_0000..0b1100_0000 => {
                bytes_full[6..].copy_from_slice(&buffer[..2]);
                bytes_full[6] &= 0b0011_1111;
            }
            0b1100_0000..0b1110_0000 => {
                bytes_full[5..].copy_from_slice(&buffer[..3]);
                bytes_full[5] &= 0b0001_1111;
            }
            0b1110_0000..0b1111_0000 => {
                bytes_full[4..].copy_from_slice(&buffer[..4]);
                bytes_full[4] &= 0b0000_1111;
            }
            0b1111_0000..0b1111_1000 => {
                bytes_full[3..].copy_from_slice(&buffer[..5]);
                bytes_full[3] &= 0b0000_0111;
            }
            0b1111_1000..0b1111_1100 => {
                bytes_full[2..].copy_from_slice(&buffer[..6]);
                bytes_full[2] &= 0b0000_0011;
            }
            0b1111_1100..0b1111_1110 => {
                bytes_full[1..].copy_from_slice(&buffer[..7]);
                bytes_full[1] &= 0b0000_0001;
            }
            0b1111_1110..0b1111_1111 => {
                bytes_full[1..].copy_from_slice(&buffer[1..8]);
            }
            0b1111_1111 => bytes_full.copy_from_slice(&buffer[1..9]),
        }
        Self(u64::from_be_bytes(bytes_full))
    }
}

impl From<usize> for Varlength {
    fn from(value: usize) -> Self {
        Self(value as u64)
    }
}

impl From<u64> for Varlength {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<&Varlength> for usize {
    fn from(value: &Varlength) -> Self {
        value.0 as usize
    }
}

impl From<&Varlength> for u64 {
    fn from(value: &Varlength) -> Self {
        value.0
    }
}
