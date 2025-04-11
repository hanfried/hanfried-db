pub mod fixed_length_counts;
pub mod fixed_length_integers;
pub mod varchar;
pub mod varcount;
pub mod varint;
pub mod varpair;

pub trait HfdbSerializableDatatype {
    fn serialized_length(&self) -> usize;
    fn serialize(&self, buffer: &mut [u8]);
    fn deserialize(buffer: &[u8]) -> Self;
}

#[cfg(test)]
mod tests {
    use crate::datatypes::fixed_length_counts::{
        BigCount, Count, HugeCount, SmallCount, TinyCount,
    };
    use crate::datatypes::fixed_length_integers::{
        BigInteger, HugeInteger, Integer, SmallInteger, TinyInteger,
    };
    use crate::datatypes::varchar::Varchar;
    use crate::datatypes::varcount::Varcount;
    use crate::datatypes::varint::Varint;
    use crate::datatypes::varpair::Varpair;
    use crate::datatypes::HfdbSerializableDatatype;
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
            check_serialize_deserialize(&mut buffer, Varcount::from(nth_power - 1));
            check_serialize_deserialize(&mut buffer, Varcount::from(nth_power));
            check_serialize_deserialize(&mut buffer, Varcount::from(nth_power + 1));
        }
        check_serialize_deserialize(&mut buffer, Varcount::from(usize::MAX));
    }

    #[test]
    fn test_serialize_deserialize_varint() {
        let mut buffer = [0u8; 9];

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

    #[test]
    fn test_serialize_deserialize_varchar() {
        let mut buffer = [0u8; 100];
        check_serialize_deserialize(&mut buffer, Varchar::from(""));
        check_serialize_deserialize(&mut buffer, Varchar::from("abc"));
    }

    #[test]
    fn test_serialize_deserialize_varpair() {
        let mut buffer = [0u8; 100];
        check_serialize_deserialize(
            &mut buffer,
            Varpair::from((Varcount::from(42usize), Varchar::from(""))),
        );
    }

    #[test]
    fn test_serialize_deserialize_fixed_length_integers() {
        let mut buffer = [0u8; 100];
        check_serialize_deserialize(&mut buffer, TinyInteger::from(0));
        check_serialize_deserialize(&mut buffer, TinyInteger::from(i8::MIN));
        check_serialize_deserialize(&mut buffer, TinyInteger::from(i8::MAX));

        check_serialize_deserialize(&mut buffer, SmallInteger::from(0));
        check_serialize_deserialize(&mut buffer, SmallInteger::from(i16::MIN));
        check_serialize_deserialize(&mut buffer, SmallInteger::from(i16::MAX));

        check_serialize_deserialize(&mut buffer, Integer::from(0));
        check_serialize_deserialize(&mut buffer, Integer::from(i32::MIN));
        check_serialize_deserialize(&mut buffer, Integer::from(i32::MAX));

        check_serialize_deserialize(&mut buffer, BigInteger::from(0));
        check_serialize_deserialize(&mut buffer, BigInteger::from(i64::MIN));
        check_serialize_deserialize(&mut buffer, BigInteger::from(i64::MAX));

        check_serialize_deserialize(&mut buffer, HugeInteger::from(0));
        check_serialize_deserialize(&mut buffer, HugeInteger::from(i128::MIN));
        check_serialize_deserialize(&mut buffer, HugeInteger::from(i128::MAX));
    }

    #[test]
    fn test_serialize_deserialize_fixed_length_counts() {
        let mut buffer = [0u8; 100];
        check_serialize_deserialize(&mut buffer, TinyCount::from(0));
        check_serialize_deserialize(&mut buffer, TinyCount::from(u8::MIN));
        check_serialize_deserialize(&mut buffer, TinyCount::from(u8::MAX));

        check_serialize_deserialize(&mut buffer, SmallCount::from(0));
        check_serialize_deserialize(&mut buffer, SmallCount::from(u16::MIN));
        check_serialize_deserialize(&mut buffer, SmallCount::from(u16::MAX));

        check_serialize_deserialize(&mut buffer, Count::from(0));
        check_serialize_deserialize(&mut buffer, Count::from(u32::MIN));
        check_serialize_deserialize(&mut buffer, Count::from(u32::MAX));

        check_serialize_deserialize(&mut buffer, BigCount::from(0));
        check_serialize_deserialize(&mut buffer, BigCount::from(u64::MIN));
        check_serialize_deserialize(&mut buffer, BigCount::from(u64::MAX));

        check_serialize_deserialize(&mut buffer, HugeCount::from(0));
        check_serialize_deserialize(&mut buffer, HugeCount::from(u128::MIN));
        check_serialize_deserialize(&mut buffer, HugeCount::from(u128::MAX));
    }
}
