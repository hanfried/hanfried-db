use crate::datatypes::HfdbSerializableDatatype;

#[derive(Debug, Eq, PartialEq)]
pub struct Varpair<T, U>
where
    T: HfdbSerializableDatatype,
    U: HfdbSerializableDatatype,
{
    left: T,
    right: U,
}

impl<T, U> HfdbSerializableDatatype for Varpair<T, U>
where
    T: HfdbSerializableDatatype,
    U: HfdbSerializableDatatype,
{
    fn serialized_length(&self) -> usize {
        self.left.serialized_length() + self.right.serialized_length()
    }

    fn serialize(&self, buffer: &mut [u8]) {
        self.left.serialize(buffer);
        self.right
            .serialize(&mut buffer[self.left.serialized_length()..])
    }

    fn deserialize(buffer: &[u8]) -> Self {
        let left = T::deserialize(buffer);
        let right = U::deserialize(&buffer[left.serialized_length()..]);
        Self { left, right }
    }
}

impl<T, U> From<(T, U)> for Varpair<T, U>
where
    T: HfdbSerializableDatatype,
    U: HfdbSerializableDatatype,
{
    fn from(value: (T, U)) -> Self {
        Self {
            left: value.0,
            right: value.1,
        }
    }
}
