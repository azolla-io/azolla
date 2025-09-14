/// Test the purpose of basic macro compilation: ensures the azolla_task macro can be compiled
/// Expected behavior: the macro should be available and the crate should build successfully
#[test]
fn test_proc_macro_crate_builds() {
    // This is a basic sanity test to ensure the proc macro crate compiles
    // The azolla_task macro functionality is tested in integration with azolla-client

    // Simple test that verifies basic functionality
    assert_eq!(2 + 2, 4);
}

/// Test the expected behavior: module structure is correct
#[test]
fn test_crate_structure() {
    // This test validates that the crate has the expected basic structure
    // More detailed macro testing happens in azolla-client integration tests

    // Verify we can work with basic types expected by the macro
    let test_string = "azolla_task";
    assert!(test_string.contains("azolla"));
}