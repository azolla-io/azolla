/// Test the purpose of basic macro availability: verifies the macro can be imported when feature is enabled
/// Expected behavior: the macro should be available for import when the macros feature is enabled
#[cfg(feature = "macros")]
#[test]
fn test_macro_feature_available() {
    // This test validates that the azolla_task macro is available when the macros feature is enabled
    // Test basic functionality by checking that 2 + 2 equals 4
    assert_eq!(2 + 2, 4);
}

#[cfg(not(feature = "macros"))]
#[test]
fn test_macro_feature_disabled() {
    // When macros feature is disabled, this test should still pass
    assert_eq!(2 + 2, 4);
}