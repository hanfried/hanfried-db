pub trait HfdbSerializableDatatype {
    fn serialized_length(&self) -> usize;
    fn serialize(&self, buffer: &mut [u8]);
    fn deserialize(buffer: &[u8]) -> Self;
}

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

#[derive(Debug, Eq, PartialEq)]
pub struct Varint(i64);

fn fill_up_leading_zero_or_ones_for_two_complement(buffer: &mut [u8], leading_bits: u8) {
    let highest_byte_nr = 8 - leading_bits;
    let highest_byte = buffer[highest_byte_nr as usize];
    let negative_number = (highest_byte & (0b1000_0000 >> leading_bits)) > 0;
    buffer[highest_byte_nr as usize] = if negative_number {
        match leading_bits {
            1 => highest_byte | 0b1000_0000,
            2 => highest_byte | 0b1100_0000,
            3 => highest_byte | 0b1110_0000,
            4 => highest_byte | 0b1111_0000,
            5 => highest_byte | 0b1111_1000,
            6 => highest_byte | 0b1111_1100,
            7 => highest_byte | 0b1111_1110,
            8 => 0b1111_1111,
            _ => panic!("leading bits should be between 1..=8"),
        }
    } else {
        match leading_bits {
            1 => highest_byte & 0b0111_1111,
            2 => highest_byte & 0b0011_1111,
            3 => highest_byte & 0b0001_1111,
            4 => highest_byte & 0b0000_1111,
            5 => highest_byte & 0b0000_0111,
            6 => highest_byte & 0b0000_0011,
            7 => highest_byte & 0b0000_0001,
            8 => 0b0000_0000,
            _ => panic!("leading bits should be between 1..=8"),
        }
    };
    for idx in 0..highest_byte_nr {
        buffer[idx as usize] = if negative_number { 0b1111_1111 } else { 0u8 };
    }
}
impl HfdbSerializableDatatype for Varint {
    fn serialized_length(&self) -> usize {
        for leading_bits in 1..=8 {
            let power_7bit_nth_with_1_bit_less_for_plus_minus = 1 << ((leading_bits * 7) - 1);
            if (power_7bit_nth_with_1_bit_less_for_plus_minus <= self.0 && self.0 < 0)
                || (self.0 >= 0 && self.0 < power_7bit_nth_with_1_bit_less_for_plus_minus)
            {
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
                1 => buffer[0] &= 0b0111_1111,
                2 => {
                    buffer[0] &= 0b1011_1111;
                    buffer[0] |= 0b1000_0000
                }
                3 => {
                    buffer[0] &= 0b1101_1111;
                    buffer[0] |= 0b1100_0000
                }
                4 => {
                    buffer[0] &= 0b1110_1111;
                    buffer[0] |= 0b1110_0000
                }
                5 => {
                    buffer[0] &= 0b1111_0111;
                    buffer[0] |= 0b1111_0000
                }
                6 => {
                    buffer[0] &= 0b1111_1011;
                    buffer[0] |= 0b1111_1000
                }
                7 => {
                    buffer[0] &= 0b1110_1101;
                    buffer[0] |= 0b1111_1100
                }
                8 => buffer[0] = 0b1111_1110,
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
            0b0000_0000..0b1000_0000 => {
                bytes_full[7] = buffer[0];
                fill_up_leading_zero_or_ones_for_two_complement(&mut bytes_full, 1)
            }
            0b1000_0000..0b1100_0000 => {
                bytes_full[6..].copy_from_slice(&buffer[..2]);
                fill_up_leading_zero_or_ones_for_two_complement(&mut bytes_full, 2);
            }
            0b1100_0000..0b1110_0000 => {
                bytes_full[5..].copy_from_slice(&buffer[..3]);
                fill_up_leading_zero_or_ones_for_two_complement(&mut bytes_full, 3);
            }
            0b1110_0000..0b1111_0000 => {
                bytes_full[4..].copy_from_slice(&buffer[..4]);
                fill_up_leading_zero_or_ones_for_two_complement(&mut bytes_full, 4);
            }
            0b1111_0000..0b1111_1000 => {
                bytes_full[3..].copy_from_slice(&buffer[..5]);
                fill_up_leading_zero_or_ones_for_two_complement(&mut bytes_full, 5);
            }
            0b1111_1000..0b1111_1100 => {
                bytes_full[2..].copy_from_slice(&buffer[..6]);
                fill_up_leading_zero_or_ones_for_two_complement(&mut bytes_full, 6);
            }
            0b1111_1100..0b1111_1110 => {
                bytes_full[1..].copy_from_slice(&buffer[..7]);
                fill_up_leading_zero_or_ones_for_two_complement(&mut bytes_full, 7);
            }
            0b1111_1110..0b1111_1111 => {
                bytes_full[1..].copy_from_slice(&buffer[1..8]);
                bytes_full[0] = if buffer[1] <= 127 {
                    0b0000_0000
                } else {
                    0b1111_1111
                }
            }
            0b1111_1111 => bytes_full.copy_from_slice(&buffer[1..9]),
        }

        Self(i64::from_be_bytes(bytes_full))
    }
}

impl From<i64> for Varint {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod tests {
    use crate::file_management::datatypes::{HfdbSerializableDatatype, Varint, Varlength};
    use std::fmt::Debug;

    fn check_serialize_deserialize<T>(buffer: &mut [u8], value: T)
    where
        T: HfdbSerializableDatatype + Debug + Eq,
    {
        value.serialize(buffer);
        println!(
            "Check serialization of {:?} serialized length {:?} buffer={:?}",
            value,
            value.serialized_length(),
            &buffer
        );
        assert_eq!(
            T::deserialize(buffer),
            value,
            "Check serialization of {:?}",
            value
        );
        let mut vec_with_serialized_length = vec![0u8; value.serialized_length()];
        value.serialize(vec_with_serialized_length.as_mut_slice());
        assert_eq!(
            T::deserialize(vec_with_serialized_length.as_slice()),
            value,
            "Check serialization of {:?}",
            value
        );
    }

    #[test]
    fn test_serialize_deserialize_varlength() {
        let mut buffer = [0u8; 9];
        for power in 1..64 {
            let nth_power: u64 = 1u64 << power;
            println!("nth_power {nth_power} power = {power}");
            check_serialize_deserialize(&mut buffer, Varlength::from(nth_power - 1));
            check_serialize_deserialize(&mut buffer, Varlength::from(nth_power));
            check_serialize_deserialize(&mut buffer, Varlength::from(nth_power + 1));
        }
        check_serialize_deserialize(&mut buffer, Varlength::from(usize::MAX));
    }

    #[test]
    fn test_serialize_deserialize_varint() {
        let mut buffer = [0u8; 9];

        check_serialize_deserialize(&mut buffer, Varint::from((1 << 55) - 1));

        for power in 1..63 {
            let nth_power: u64 = 1u64 << power;
            println!("nth_power {nth_power} power = {power}");

            let nth_power_positive = nth_power as i64;
            check_serialize_deserialize(&mut buffer, Varint::from(nth_power_positive - 1));
            check_serialize_deserialize(&mut buffer, Varint::from(nth_power_positive));
            check_serialize_deserialize(&mut buffer, Varint::from(nth_power_positive + 1));

            let nth_power_negative: i64 = -(nth_power as i64);
            check_serialize_deserialize(&mut buffer, Varint::from(nth_power_negative + 1));
            check_serialize_deserialize(&mut buffer, Varint::from(nth_power_negative));
            check_serialize_deserialize(&mut buffer, Varint::from(nth_power_negative - 1));
        }

        check_serialize_deserialize(&mut buffer, Varint::from(i64::MIN));
        check_serialize_deserialize(&mut buffer, Varint::from(i64::MAX));
    }
}
