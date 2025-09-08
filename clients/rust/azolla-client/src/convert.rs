use serde_json::Value;

/// Trait to convert JSON values to typed values
pub trait FromJsonValue: Sized {
    fn try_from(value: Value) -> Result<Self, ConversionError>;
}

#[derive(Debug, Clone)]
pub struct ConversionError {
    pub message: String,
}

impl ConversionError {
    pub fn type_mismatch(expected: &str, actual: &Value) -> Self {
        Self {
            message: format!("Expected {expected}, got {actual:?}"),
        }
    }
}

impl std::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ConversionError {}

// Implement FromJsonValue for common types
impl FromJsonValue for String {
    fn try_from(value: Value) -> Result<Self, ConversionError> {
        match value {
            Value::String(s) => Ok(s),
            _ => Err(ConversionError::type_mismatch("String", &value)),
        }
    }
}

impl FromJsonValue for i32 {
    fn try_from(value: Value) -> Result<Self, ConversionError> {
        match &value {
            Value::Number(n) => n
                .as_i64()
                .and_then(|i| i.try_into().ok())
                .ok_or_else(|| ConversionError::type_mismatch("i32", &value)),
            _ => Err(ConversionError::type_mismatch("i32", &value)),
        }
    }
}

impl FromJsonValue for i64 {
    fn try_from(value: Value) -> Result<Self, ConversionError> {
        match &value {
            Value::Number(n) => n
                .as_i64()
                .ok_or_else(|| ConversionError::type_mismatch("i64", &value)),
            _ => Err(ConversionError::type_mismatch("i64", &value)),
        }
    }
}

impl FromJsonValue for u32 {
    fn try_from(value: Value) -> Result<Self, ConversionError> {
        match &value {
            Value::Number(n) => n
                .as_u64()
                .and_then(|i| i.try_into().ok())
                .ok_or_else(|| ConversionError::type_mismatch("u32", &value)),
            _ => Err(ConversionError::type_mismatch("u32", &value)),
        }
    }
}

impl FromJsonValue for u64 {
    fn try_from(value: Value) -> Result<Self, ConversionError> {
        match &value {
            Value::Number(n) => n
                .as_u64()
                .ok_or_else(|| ConversionError::type_mismatch("u64", &value)),
            _ => Err(ConversionError::type_mismatch("u64", &value)),
        }
    }
}

impl FromJsonValue for f64 {
    fn try_from(value: Value) -> Result<Self, ConversionError> {
        match value {
            Value::Number(n) => Ok(n.as_f64().unwrap_or(0.0)),
            _ => Err(ConversionError::type_mismatch("f64", &value)),
        }
    }
}

impl FromJsonValue for bool {
    fn try_from(value: Value) -> Result<Self, ConversionError> {
        match value {
            Value::Bool(b) => Ok(b),
            _ => Err(ConversionError::type_mismatch("bool", &value)),
        }
    }
}

impl<T: FromJsonValue> FromJsonValue for Option<T> {
    fn try_from(value: Value) -> Result<Self, ConversionError> {
        match value {
            Value::Null => Ok(None),
            _ => Ok(Some(T::try_from(value)?)),
        }
    }
}

impl<T: FromJsonValue> FromJsonValue for Vec<T> {
    fn try_from(value: Value) -> Result<Self, ConversionError> {
        match value {
            Value::Array(arr) => arr
                .into_iter()
                .map(T::try_from)
                .collect::<Result<Vec<_>, _>>(),
            _ => Err(ConversionError::type_mismatch("Array", &value)),
        }
    }
}

// Implementation for serde_json::Value (passthrough)
impl FromJsonValue for Value {
    fn try_from(value: Value) -> Result<Self, ConversionError> {
        Ok(value)
    }
}
