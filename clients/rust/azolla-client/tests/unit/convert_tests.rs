//! Unit tests for JSON conversion functionality
//! Tests the FromJsonValue trait and type conversions

use azolla_client::convert::FromJsonValue;
use serde_json::{json, Value};

/// Test the purpose of basic type conversion from JSON values
/// Expected behavior: should convert JSON values to Rust types correctly
#[test]
fn test_basic_type_conversions() {
    // Test i32 conversion
    let json_int = json!(42);
    let result: Result<i32, _> = FromJsonValue::try_from(json_int);
    assert_eq!(result.unwrap(), 42);

    // Test String conversion
    let json_str = json!("hello");
    let result: Result<String, _> = FromJsonValue::try_from(json_str);
    assert_eq!(result.unwrap(), "hello");

    // Test bool conversion
    let json_bool = json!(true);
    let result: Result<bool, _> = FromJsonValue::try_from(json_bool);
    assert!(result.unwrap());
}

/// Test the expected behavior: Vec<T> conversion from JSON arrays
#[test]
fn test_vec_conversions() {
    // Test Vec<i32> conversion
    let json_array = json!([1, 2, 3, 4]);
    let result: Result<Vec<i32>, _> = FromJsonValue::try_from(json_array);
    assert_eq!(result.unwrap(), vec![1, 2, 3, 4]);

    // Test Vec<String> conversion
    let json_str_array = json!(["a", "b", "c"]);
    let result: Result<Vec<String>, _> = FromJsonValue::try_from(json_str_array);
    assert_eq!(result.unwrap(), vec!["a", "b", "c"]);

    // Test empty Vec conversion
    let empty_array = json!([]);
    let result: Result<Vec<i32>, _> = FromJsonValue::try_from(empty_array);
    assert_eq!(result.unwrap(), Vec::<i32>::new());
}

/// Test the purpose of Option<T> conversion handling
/// Expected behavior: should handle null values and present values correctly
#[test]
fn test_option_conversions() {
    // Test Some value conversion
    let json_value = json!(42);
    let result: Result<Option<i32>, _> = FromJsonValue::try_from(json_value);
    assert_eq!(result.unwrap(), Some(42));

    // Test null conversion
    let json_null = json!(null);
    let result: Result<Option<i32>, _> = FromJsonValue::try_from(json_null);
    assert_eq!(result.unwrap(), None);

    // Test nested Option<String>
    let json_str = json!("test");
    let result: Result<Option<String>, _> = FromJsonValue::try_from(json_str);
    assert_eq!(result.unwrap(), Some("test".to_string()));
}

/// Test the expected behavior: error handling for invalid conversions
#[test]
fn test_conversion_errors() {
    // Test string to number conversion error
    let json_str = json!("not_a_number");
    let result: Result<i32, _> = FromJsonValue::try_from(json_str);
    assert!(result.is_err());

    // Test number to string conversion error
    let json_num = json!(42);
    let result: Result<String, _> = FromJsonValue::try_from(json_num);
    assert!(result.is_err());

    // Test array to single value conversion error
    let json_array = json!([1, 2, 3]);
    let result: Result<i32, _> = FromJsonValue::try_from(json_array);
    assert!(result.is_err());
}

/// Test the purpose of nested structure conversion
/// Expected behavior: should handle complex nested JSON structures
#[test]
fn test_nested_conversions() {
    // Test nested Vec<Vec<i32>>
    let nested_array = json!([[1, 2], [3, 4], [5, 6]]);
    let result: Result<Vec<Vec<i32>>, _> = FromJsonValue::try_from(nested_array);
    assert_eq!(result.unwrap(), vec![vec![1, 2], vec![3, 4], vec![5, 6]]);

    // Test Vec<Option<i32>>
    let mixed_array = json!([1, null, 3]);
    let result: Result<Vec<Option<i32>>, _> = FromJsonValue::try_from(mixed_array);
    assert_eq!(result.unwrap(), vec![Some(1), None, Some(3)]);
}

/// Test the expected behavior: numeric type boundaries
#[test]
fn test_numeric_boundaries() {
    // Test i32 max value
    let json_max = json!(i32::MAX);
    let result: Result<i32, _> = FromJsonValue::try_from(json_max);
    assert_eq!(result.unwrap(), i32::MAX);

    // Test i32 min value
    let json_min = json!(i32::MIN);
    let result: Result<i32, _> = FromJsonValue::try_from(json_min);
    assert_eq!(result.unwrap(), i32::MIN);

    // Test u32 conversion
    let json_uint = json!(42u32);
    let result: Result<u32, _> = FromJsonValue::try_from(json_uint);
    assert_eq!(result.unwrap(), 42u32);
}

/// Test the purpose of i64 conversion edge cases
/// Expected behavior: should handle large numbers and overflow scenarios
#[test]
fn test_i64_conversion_edge_cases() {
    // Test large i64 value
    let large_int = json!(9223372036854775807i64);
    let result: Result<i64, _> = FromJsonValue::try_from(large_int);
    assert_eq!(result.unwrap(), 9223372036854775807i64);

    // Test float to i64 conversion (should fail)
    let float_value = json!(42.5);
    let result: Result<i64, _> = FromJsonValue::try_from(float_value);
    assert!(result.is_err());

    // Test very large float (should fail for i64)
    let large_float = json!(1.23e100);
    let result: Result<i64, _> = FromJsonValue::try_from(large_float);
    assert!(result.is_err());
}

/// Test the expected behavior: string edge cases
#[test]
fn test_string_edge_cases() {
    // Test empty string
    let empty_str = json!("");
    let result: Result<String, _> = FromJsonValue::try_from(empty_str);
    assert_eq!(result.unwrap(), "");

    // Test string with special characters
    let special_str = json!("Hello\n\t\"World\"");
    let result: Result<String, _> = FromJsonValue::try_from(special_str);
    assert_eq!(result.unwrap(), "Hello\n\t\"World\"");

    // Test Unicode string
    let unicode_str = json!("Hello 🦀 World");
    let result: Result<String, _> = FromJsonValue::try_from(unicode_str);
    assert_eq!(result.unwrap(), "Hello 🦀 World");
}

/// Test the purpose of mixed array element handling
/// Expected behavior: should handle type mismatches in arrays gracefully
#[test]
fn test_mixed_array_elements() {
    // Test array with mixed types (should fail for Vec<i32>)
    let mixed_array = json!([1, "two", 3]);
    let result: Result<Vec<i32>, _> = FromJsonValue::try_from(mixed_array);
    assert!(result.is_err());

    // Test array with some invalid elements for Vec<Option<i32>>
    let partially_valid = json!([1, null, "invalid", 4]);
    let result: Result<Vec<Option<i32>>, _> = FromJsonValue::try_from(partially_valid);
    assert!(result.is_err());
}

/// Test the expected behavior: Value passthrough
/// Expected behavior: JSON Value should pass through unchanged
#[test]
fn test_value_passthrough() {
    // Test that Value passes through unchanged
    let original = json!({"key": "value", "number": 42, "array": [1, 2, 3]});
    let result: Result<Value, _> = FromJsonValue::try_from(original.clone());
    assert_eq!(result.unwrap(), original);

    // Test null Value
    let null_value = json!(null);
    let result: Result<Value, _> = FromJsonValue::try_from(null_value.clone());
    assert_eq!(result.unwrap(), null_value);
}
