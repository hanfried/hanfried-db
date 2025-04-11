pub mod varchar;
pub mod varint;
pub mod varlength;
mod varpair;

pub trait HfdbSerializableDatatype {
    fn serialized_length(&self) -> usize;
    fn serialize(&self, buffer: &mut [u8]);
    fn deserialize(buffer: &[u8]) -> Self;
}

#[cfg(test)]
mod tests {
    use crate::datatypes::varchar::Varchar;
    use crate::datatypes::varint::Varint;
    use crate::datatypes::varlength::Varlength;
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
            check_serialize_deserialize(&mut buffer, Varlength::from(nth_power - 1));
            check_serialize_deserialize(&mut buffer, Varlength::from(nth_power));
            check_serialize_deserialize(&mut buffer, Varlength::from(nth_power + 1));
        }
        check_serialize_deserialize(&mut buffer, Varlength::from(usize::MAX));
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
            Varpair::from((Varlength::from(42usize), Varchar::from(""))),
        );
    }
}
