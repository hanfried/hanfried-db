use crate::datatypes::varlength::Varlength;
use crate::datatypes::HfdbSerializableDatatype;

#[derive(Debug, Eq, PartialEq)]
pub struct Varchar {
    varlength: Varlength,
    data: String,
}

impl HfdbSerializableDatatype for Varchar {
    fn serialized_length(&self) -> usize {
        self.varlength.serialized_length() + self.data.len()
    }

    fn serialize(&self, buffer: &mut [u8]) {
        self.varlength.serialize(buffer);
        let offset_str = self.varlength.serialized_length();
        buffer[offset_str..offset_str + self.data.len()].copy_from_slice(self.data.as_bytes());
    }

    fn deserialize(buffer: &[u8]) -> Self {
        let varlength = Varlength::deserialize(buffer);
        let offset_str = varlength.serialized_length();
        let data =
            String::from_utf8_lossy(&buffer[offset_str..offset_str + usize::from(&varlength)])
                .to_string();
        Self { varlength, data }
    }
}

impl From<String> for Varchar {
    fn from(value: String) -> Self {
        Self {
            varlength: Varlength::from(value.len()),
            data: value,
        }
    }
}

impl From<&str> for Varchar {
    fn from(value: &str) -> Self {
        Self {
            varlength: Varlength::from(value.len()),
            data: value.to_string(),
        }
    }
}

impl From<&Varchar> for String {
    fn from(value: &Varchar) -> Self {
        value.data.clone()
    }
}
