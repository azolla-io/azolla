use serde_json::Value;

/// Trait to convert JSON values to typed values
pub trait FromJsonValue: Sized {
    fn try_from(value: Value) -> Result<Self, ConversionError>;
}

#[derive(Debug, Clone)]
pub enum ConversionError {
    TypeMismatch { expected: String, actual: String },
    Other(String),
}

impl ConversionError {
    pub fn type_mismatch(expected: &str, actual: &Value) -> Self {
        Self::TypeMismatch {
            expected: expected.to_string(),
            actual: format!("{actual:?}"),
        }
    }
}

impl std::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConversionError::TypeMismatch { expected, actual } => {
                write!(f, "Type mismatch: expected {expected}, got {actual}")
            }
            ConversionError::Other(msg) => write!(f, "Conversion error: {msg}"),
        }
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
            Value::Number(n) => n
                .as_f64()
                .ok_or_else(|| ConversionError::type_mismatch("f64", &Value::Number(n))),
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Test the purpose of FromJsonValue: ensure basic type conversions work correctly
    #[test]
    fn test_from_json_value_basic_types() {
        // Test i32
        assert_eq!(<i32 as FromJsonValue>::try_from(json!(42)).unwrap(), 42);
        assert_eq!(<i32 as FromJsonValue>::try_from(json!(-123)).unwrap(), -123);
        assert_eq!(<i32 as FromJsonValue>::try_from(json!(0)).unwrap(), 0);
        assert!(<i32 as FromJsonValue>::try_from(json!("not a number")).is_err());
        assert!(<i32 as FromJsonValue>::try_from(json!(42.5)).is_err());
        assert!(<i32 as FromJsonValue>::try_from(json!(true)).is_err());

        // Test u32
        assert_eq!(<u32 as FromJsonValue>::try_from(json!(42)).unwrap(), 42);
        assert_eq!(<u32 as FromJsonValue>::try_from(json!(0)).unwrap(), 0);
        assert!(<u32 as FromJsonValue>::try_from(json!(-123)).is_err()); // Negative should fail
        assert!(<u32 as FromJsonValue>::try_from(json!("not a number")).is_err());

        // Test f64
        assert_eq!(<f64 as FromJsonValue>::try_from(json!(42.5)).unwrap(), 42.5);
        assert_eq!(<f64 as FromJsonValue>::try_from(json!(42)).unwrap(), 42.0);
        assert_eq!(<f64 as FromJsonValue>::try_from(json!(0.0)).unwrap(), 0.0);
        assert!(<f64 as FromJsonValue>::try_from(json!("not a number")).is_err());

        // Test String
        assert_eq!(
            <String as FromJsonValue>::try_from(json!("hello")).unwrap(),
            "hello"
        );
        assert_eq!(<String as FromJsonValue>::try_from(json!("")).unwrap(), "");
        assert!(<String as FromJsonValue>::try_from(json!(42)).is_err());
        assert!(<String as FromJsonValue>::try_from(json!(true)).is_err());

        // Test bool
        assert!(<bool as FromJsonValue>::try_from(json!(true)).unwrap());
        assert!(!<bool as FromJsonValue>::try_from(json!(false)).unwrap());
        assert!(<bool as FromJsonValue>::try_from(json!("true")).is_err());
        assert!(<bool as FromJsonValue>::try_from(json!(1)).is_err());
    }

    /// Test the expected behavior: Option<T> handling
    #[test]
    fn test_from_json_value_option() {
        // Test Option<i32>
        assert_eq!(
            <Option<i32> as FromJsonValue>::try_from(json!(null)).unwrap(),
            None
        );
        assert_eq!(
            <Option<i32> as FromJsonValue>::try_from(json!(42)).unwrap(),
            Some(42)
        );
        assert!(<Option<i32> as FromJsonValue>::try_from(json!("invalid")).is_err());

        // Test Option<String>
        assert_eq!(
            <Option<String> as FromJsonValue>::try_from(json!(null)).unwrap(),
            None
        );
        assert_eq!(
            <Option<String> as FromJsonValue>::try_from(json!("hello")).unwrap(),
            Some("hello".to_string())
        );
        assert!(<Option<String> as FromJsonValue>::try_from(json!(42)).is_err());

        // Test Option<bool>
        assert_eq!(
            <Option<bool> as FromJsonValue>::try_from(json!(null)).unwrap(),
            None
        );
        assert_eq!(
            <Option<bool> as FromJsonValue>::try_from(json!(true)).unwrap(),
            Some(true)
        );
        assert_eq!(
            <Option<bool> as FromJsonValue>::try_from(json!(false)).unwrap(),
            Some(false)
        );
    }

    /// Test Vec<T> conversions
    #[test]
    fn test_from_json_value_vec() {
        // Test Vec<i32>
        let result = <Vec<i32> as FromJsonValue>::try_from(json!([1, 2, 3])).unwrap();
        assert_eq!(result, vec![1, 2, 3]);

        let empty_result = <Vec<i32> as FromJsonValue>::try_from(json!([])).unwrap();
        assert_eq!(empty_result, Vec::<i32>::new());

        assert!(<Vec<i32> as FromJsonValue>::try_from(json!("not an array")).is_err());
        assert!(<Vec<i32> as FromJsonValue>::try_from(json!([1, "invalid", 3])).is_err());

        // Test Vec<String>
        let string_result =
            <Vec<String> as FromJsonValue>::try_from(json!(["hello", "world"])).unwrap();
        assert_eq!(
            string_result,
            vec!["hello".to_string(), "world".to_string()]
        );

        // Test Vec<Option<i32>>
        let optional_result =
            <Vec<Option<i32>> as FromJsonValue>::try_from(json!([1, null, 3])).unwrap();
        assert_eq!(optional_result, vec![Some(1), None, Some(3)]);
    }

    /// Test nested Vec<Vec<T>> structures
    #[test]
    fn test_from_json_value_nested_vec() {
        let nested_data = json!([[1, 2], [3, 4], []]);
        let result = <Vec<Vec<i32>> as FromJsonValue>::try_from(nested_data).unwrap();
        assert_eq!(result, vec![vec![1, 2], vec![3, 4], vec![]]);
    }

    /// Test ConversionError creation and display
    #[test]
    fn test_conversion_error() {
        let error = ConversionError::type_mismatch("i32", &json!("string"));
        assert!(error.to_string().contains("expected i32"));
        assert!(error.to_string().contains("got"));

        let other_error = ConversionError::Other("Custom error".to_string());
        assert_eq!(other_error.to_string(), "Conversion error: Custom error");
    }

    /// Test error messages are descriptive
    #[test]
    fn test_conversion_error_messages() {
        // Test various type mismatches
        let int_error = <i32 as FromJsonValue>::try_from(json!("not_an_int")).unwrap_err();
        assert!(
            int_error.to_string().contains("expected i32")
                || int_error.to_string().contains("type")
        );

        let string_error = <String as FromJsonValue>::try_from(json!(42)).unwrap_err();
        assert!(
            string_error.to_string().contains("expected String")
                || string_error.to_string().contains("type")
        );

        let bool_error = <bool as FromJsonValue>::try_from(json!("not_bool")).unwrap_err();
        assert!(
            bool_error.to_string().contains("expected bool")
                || bool_error.to_string().contains("type")
        );

        let array_error = <Vec<i32> as FromJsonValue>::try_from(json!("not_array")).unwrap_err();
        assert!(
            array_error.to_string().contains("expected Array")
                || array_error.to_string().contains("type")
        );
    }

    /// Test numeric edge cases and overflow
    #[test]
    fn test_numeric_edge_cases() {
        // Test i32 bounds
        assert_eq!(
            <i32 as FromJsonValue>::try_from(json!(i32::MAX)).unwrap(),
            i32::MAX
        );
        assert_eq!(
            <i32 as FromJsonValue>::try_from(json!(i32::MIN)).unwrap(),
            i32::MIN
        );

        // Test u32 bounds
        assert_eq!(
            <u32 as FromJsonValue>::try_from(json!(u32::MAX)).unwrap(),
            u32::MAX
        );
        assert_eq!(<u32 as FromJsonValue>::try_from(json!(0)).unwrap(), 0);

        // Test overflow cases
        assert!(<i32 as FromJsonValue>::try_from(json!(i64::MAX)).is_err());
        assert!(<u32 as FromJsonValue>::try_from(json!(u64::MAX)).is_err());

        // Test floating point edge cases (note: JSON can't represent infinity/NaN directly)
        // JSON serializes infinity and NaN as null, so these would fail conversion
        // Instead test with large finite numbers
        assert_eq!(
            <f64 as FromJsonValue>::try_from(json!(1e308)).unwrap(),
            1e308
        );
        assert_eq!(
            <f64 as FromJsonValue>::try_from(json!(-1e308)).unwrap(),
            -1e308
        );
        assert_eq!(
            <f64 as FromJsonValue>::try_from(json!(f64::MIN)).unwrap(),
            f64::MIN
        );
    }

    /// Test special string cases
    #[test]
    fn test_string_edge_cases() {
        // Test empty string
        assert_eq!(<String as FromJsonValue>::try_from(json!("")).unwrap(), "");

        // Test unicode strings
        assert_eq!(
            <String as FromJsonValue>::try_from(json!("🚀 Hello 世界")).unwrap(),
            "🚀 Hello 世界"
        );

        // Test strings with special characters
        let special = "Line 1\nLine 2\tTabbed\r\nWindows line ending";
        assert_eq!(
            <String as FromJsonValue>::try_from(json!(special)).unwrap(),
            special
        );

        // Test very long string
        let long_string = "a".repeat(1000);
        assert_eq!(
            <String as FromJsonValue>::try_from(json!(long_string.clone())).unwrap(),
            long_string
        );
    }

    /// Test complex nested structures
    #[test]
    fn test_complex_nested_structures() {
        // Test Vec<Option<Vec<i32>>>
        let complex_data = json!([[1, 2], null, [3, 4, 5], []]);
        let result = <Vec<Option<Vec<i32>>> as FromJsonValue>::try_from(complex_data).unwrap();
        assert_eq!(
            result,
            vec![Some(vec![1, 2]), None, Some(vec![3, 4, 5]), Some(vec![])]
        );
    }

    /// Test error propagation in nested conversions
    #[test]
    fn test_error_propagation() {
        // Test that inner conversion errors bubble up
        let invalid_nested = json!([[1, 2], ["invalid", 4]]);
        let result = <Vec<Vec<i32>> as FromJsonValue>::try_from(invalid_nested);
        assert!(result.is_err());

        // Test that Option conversion errors bubble up
        let invalid_option = json!(["not", "numbers"]);
        let result = <Option<Vec<i32>> as FromJsonValue>::try_from(invalid_option);
        assert!(result.is_err());
    }

    /// Test JSON null handling across different types
    #[test]
    fn test_null_handling() {
        // Basic types should fail on null
        assert!(<i32 as FromJsonValue>::try_from(json!(null)).is_err());
        assert!(<String as FromJsonValue>::try_from(json!(null)).is_err());
        assert!(<bool as FromJsonValue>::try_from(json!(null)).is_err());
        assert!(<Vec<i32> as FromJsonValue>::try_from(json!(null)).is_err());

        // Only Option should succeed with null
        assert_eq!(
            <Option<i32> as FromJsonValue>::try_from(json!(null)).unwrap(),
            None
        );
        assert_eq!(
            <Option<String> as FromJsonValue>::try_from(json!(null)).unwrap(),
            None
        );
        assert_eq!(
            <Option<Vec<i32>> as FromJsonValue>::try_from(json!(null)).unwrap(),
            None
        );
    }

    /// Test array with mixed valid/invalid elements
    #[test]
    fn test_mixed_array_elements() {
        // Test array with one invalid element fails entire conversion
        let mixed_array = json!([1, 2, "invalid", 4]);
        let result = <Vec<i32> as FromJsonValue>::try_from(mixed_array);
        assert!(result.is_err());

        // Test array with valid elements succeeds
        let valid_array = json!([1, 2, 3, 4]);
        let result = <Vec<i32> as FromJsonValue>::try_from(valid_array).unwrap();
        assert_eq!(result, vec![1, 2, 3, 4]);
    }

    /// Test trait bounds and generic constraints
    #[test]
    fn test_generic_constraints() {
        fn test_conversion<T: FromJsonValue + PartialEq + std::fmt::Debug>(
            value: serde_json::Value,
            expected: T,
        ) {
            let result = T::try_from(value).unwrap();
            assert_eq!(result, expected);
        }

        test_conversion(json!(42), 42i32);
        test_conversion(json!("hello"), "hello".to_string());
        test_conversion(json!(true), true);
        test_conversion(json!([1, 2, 3]), vec![1, 2, 3]);
    }

    /// Test Value passthrough
    #[test]
    fn test_value_passthrough() {
        let original = json!({"key": "value", "number": 42, "array": [1, 2, 3]});
        let converted = <Value as FromJsonValue>::try_from(original.clone()).unwrap();
        assert_eq!(original, converted);
    }

    /// Test integer type boundaries
    #[test]
    fn test_integer_type_boundaries() {
        // Test i64 bounds
        assert_eq!(
            <i64 as FromJsonValue>::try_from(json!(i64::MAX)).unwrap(),
            i64::MAX
        );
        assert_eq!(
            <i64 as FromJsonValue>::try_from(json!(i64::MIN)).unwrap(),
            i64::MIN
        );

        // Test u64 bounds
        assert_eq!(
            <u64 as FromJsonValue>::try_from(json!(u64::MAX)).unwrap(),
            u64::MAX
        );
        assert_eq!(<u64 as FromJsonValue>::try_from(json!(0u64)).unwrap(), 0u64);

        // Test negative numbers with unsigned types
        assert!(<u64 as FromJsonValue>::try_from(json!(-1)).is_err());
    }

    // Note: serde_json rejects unrepresentable numeric literals (e.g., 1e400),
    // so testing the unreachable as_f64(None) path is not feasible here.
}
