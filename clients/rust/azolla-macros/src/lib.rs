use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, FnArg, ItemFn, Pat};

/// Convert snake_case to PascalCase
fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars: Vec<char> = word.chars().collect();
            if !chars.is_empty() {
                chars[0] = chars[0].to_uppercase().next().unwrap_or(chars[0]);
            }
            chars.iter().collect::<String>()
        })
        .collect()
}

/// Proc macro to convert functions into Azolla tasks with type-safe argument extraction
#[proc_macro_attribute]
pub fn azolla_task(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();
    let fn_vis = &input_fn.vis;

    // Extract parameter information
    let mut param_types = Vec::new();
    let mut param_names = Vec::new();

    for input in input_fn.sig.inputs.iter() {
        if let FnArg::Typed(pat_type) = input {
            if let Pat::Ident(pat_ident) = pat_type.pat.as_ref() {
                param_names.push(&pat_ident.ident);
                param_types.push(&pat_type.ty);
            }
        }
    }

    // Generate wrapper struct name
    let fn_name_pascal = to_pascal_case(&fn_name_str);
    let wrapper_struct_name = syn::Ident::new(&format!("{fn_name_pascal}Task"), fn_name.span());

    // Generate the Args type and unpacking logic
    let (args_type, arg_unpacking) = match param_types.len() {
        0 => {
            // No parameters
            (quote! { () }, quote! { let _ = args; })
        }
        1 => {
            // Single parameter
            let param_type = &param_types[0];
            let param_name = &param_names[0];
            (quote! { #param_type }, quote! { let #param_name = args; })
        }
        _ => {
            // Multiple parameters - use tuple
            (
                quote! { (#(#param_types),*) },
                quote! { let (#(#param_names),*) = args; },
            )
        }
    };

    // Use a simple, reliable approach with explicit imports in generated code
    let expanded = quote! {
        // Keep the original function
        #input_fn

        // Generate wrapper struct
        #fn_vis struct #wrapper_struct_name;

        // Generate the Task implementation
        impl ::azolla_client::task::Task for #wrapper_struct_name {
            type Args = #args_type;

            fn name(&self) -> &'static str {
                #fn_name_str
            }

            fn execute(&self, args: Self::Args) -> ::std::pin::Pin<::std::boxed::Box<dyn ::std::future::Future<Output = ::azolla_client::task::TaskResult> + ::std::marker::Send + '_>> {
                ::std::boxed::Box::pin(async move {
                    // Unpack arguments
                    #arg_unpacking

                    // Call original function
                    let result = #fn_name(#(#param_names),*).await;

                    // Convert result to JSON
                    match result {
                        Ok(value) => {
                            let json_value = ::serde_json::to_value(value)
                                .map_err(|e| ::azolla_client::error::TaskError::execution_failed(
                                    &::std::format!("Failed to serialize result: {e}")
                                ))?;
                            Ok(json_value)
                        },
                        Err(e) => Err(e),
                    }
                })
            }
        }
    };

    TokenStream::from(expanded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_pascal_case_simple() {
        assert_eq!(to_pascal_case("hello"), "Hello");
        assert_eq!(to_pascal_case("world"), "World");
    }

    #[test]
    fn test_to_pascal_case_snake_case() {
        assert_eq!(to_pascal_case("hello_world"), "HelloWorld");
        assert_eq!(to_pascal_case("test_function_name"), "TestFunctionName");
        assert_eq!(to_pascal_case("a_b_c"), "ABC");
    }

    #[test]
    fn test_to_pascal_case_empty_string() {
        assert_eq!(to_pascal_case(""), "");
    }

    #[test]
    fn test_to_pascal_case_single_char() {
        assert_eq!(to_pascal_case("a"), "A");
        assert_eq!(to_pascal_case("z"), "Z");
    }

    #[test]
    fn test_to_pascal_case_already_capitalized() {
        assert_eq!(to_pascal_case("Hello"), "Hello");
        assert_eq!(to_pascal_case("Hello_World"), "HelloWorld");
    }

    #[test]
    fn test_to_pascal_case_with_numbers() {
        assert_eq!(to_pascal_case("test_123"), "Test123");
        assert_eq!(to_pascal_case("func_v2"), "FuncV2");
    }

    #[test]
    fn test_to_pascal_case_trailing_underscore() {
        assert_eq!(to_pascal_case("hello_"), "Hello");
        assert_eq!(to_pascal_case("test_func_"), "TestFunc");
    }

    #[test]
    fn test_to_pascal_case_multiple_underscores() {
        assert_eq!(to_pascal_case("hello__world"), "HelloWorld");
        assert_eq!(to_pascal_case("a___b"), "AB");
    }

    #[test]
    fn test_to_pascal_case_leading_underscore() {
        assert_eq!(to_pascal_case("_hello"), "Hello");
        assert_eq!(to_pascal_case("_hello_world"), "HelloWorld");
        assert_eq!(to_pascal_case("__test"), "Test");
    }

    #[test]
    fn test_to_pascal_case_mixed_cases() {
        assert_eq!(to_pascal_case("HeLLo_WoRLd"), "HeLLoWoRLd");
        assert_eq!(to_pascal_case("UPPER_CASE"), "UPPERCASE");
        assert_eq!(to_pascal_case("lower_case"), "LowerCase");
        assert_eq!(to_pascal_case("MiXeD_cAsE"), "MiXeDCAsE");
    }

    #[test]
    fn test_to_pascal_case_special_characters_in_words() {
        // Note: These test how the function handles non-underscore characters
        assert_eq!(to_pascal_case("hello-world"), "Hello-world"); // Hyphens stay
        assert_eq!(to_pascal_case("test.name"), "Test.name"); // Dots stay
        assert_eq!(to_pascal_case("func@name"), "Func@name"); // @ stays
    }

    #[test]
    fn test_to_pascal_case_numeric_segments() {
        assert_eq!(to_pascal_case("test_1_case"), "Test1Case");
        assert_eq!(to_pascal_case("func_v2_new"), "FuncV2New");
        assert_eq!(to_pascal_case("123_test"), "123Test");
        assert_eq!(to_pascal_case("test_123"), "Test123");
    }

    #[test]
    fn test_to_pascal_case_only_underscores() {
        assert_eq!(to_pascal_case("_"), "");
        assert_eq!(to_pascal_case("__"), "");
        assert_eq!(to_pascal_case("___"), "");
        assert_eq!(to_pascal_case("____"), "");
    }

    #[test]
    fn test_to_pascal_case_unicode_handling() {
        // Test that unicode characters are handled correctly
        assert_eq!(to_pascal_case("测试_函数"), "测试函数");
        assert_eq!(to_pascal_case("函数_名称_测试"), "函数名称测试");
        assert_eq!(to_pascal_case("émoji_🦀_test"), "Émoji🦀Test");
    }

    #[test]
    fn test_to_pascal_case_single_underscore() {
        assert_eq!(to_pascal_case("_"), "");
    }

    #[test]
    fn test_to_pascal_case_whitespace_handling() {
        // These test edge cases with actual whitespace (not underscores)
        // The function should treat these as single words
        assert_eq!(to_pascal_case(" hello world "), " hello world ");
        assert_eq!(to_pascal_case("\thello\tworld\t"), "\thello\tworld\t");
        assert_eq!(to_pascal_case("\nhello\nworld\n"), "\nhello\nworld\n");
    }

    #[test]
    fn test_to_pascal_case_very_long_input() {
        let long_input = "very_long_function_name_with_many_segments_that_tests_performance_and_correctness_of_pascal_case_conversion";
        let expected = "VeryLongFunctionNameWithManySegmentsThatTestsPerformanceAndCorrectnessOfPascalCaseConversion";
        assert_eq!(to_pascal_case(long_input), expected);
    }

    #[test]
    fn test_to_pascal_case_repeated_patterns() {
        assert_eq!(to_pascal_case("test_test_test"), "TestTestTest");
        assert_eq!(to_pascal_case("a_a_a_a"), "AAAA");
        assert_eq!(
            to_pascal_case("hello_hello_world_world"),
            "HelloHelloWorldWorld"
        );
    }

    // Test macro generation edge cases by testing the components
    #[test]
    fn test_wrapper_struct_naming_logic() {
        // Test that our PascalCase conversion works for various function names
        // that would be used in the macro
        let function_names = vec![
            ("simple_task", "SimpleTask"),
            ("complex_async_operation", "ComplexAsyncOperation"),
            ("process_data_v2", "ProcessDataV2"),
            ("handle_user_authentication", "HandleUserAuthentication"),
            ("a", "A"),
            (
                "very_long_descriptive_function_name",
                "VeryLongDescriptiveFunctionName",
            ),
        ];

        for (input, expected) in function_names {
            let pascal_case = to_pascal_case(input);
            let wrapper_name = format!("{pascal_case}Task");
            let expected_wrapper = format!("{expected}Task");
            assert_eq!(wrapper_name, expected_wrapper, "Failed for input: {input}");
        }
    }

    #[test]
    fn test_empty_word_handling() {
        // Test how the function handles empty segments between underscores
        assert_eq!(to_pascal_case("hello__world"), "HelloWorld");
        assert_eq!(to_pascal_case("test___case"), "TestCase");
        assert_eq!(to_pascal_case("a____b"), "AB");

        // Edge case: starting with underscore followed by empty segment
        assert_eq!(to_pascal_case("_test"), "Test");
        assert_eq!(to_pascal_case("__test"), "Test");
    }

    #[test]
    fn test_character_casing_edge_cases() {
        // Test various character casing scenarios
        assert_eq!(to_pascal_case("i"), "I");
        assert_eq!(to_pascal_case("test_i_case"), "TestICase");

        // Test with already uppercase first character
        assert_eq!(to_pascal_case("Test_case"), "TestCase");
        assert_eq!(to_pascal_case("TEST_CASE"), "TESTCASE");

        // Test mixed case preservation within words
        assert_eq!(to_pascal_case("testHTML_parser"), "TestHTMLParser");
        assert_eq!(
            to_pascal_case("XMLHttpRequest_handler"),
            "XMLHttpRequestHandler"
        );
    }

    #[test]
    fn test_macro_identifier_compatibility() {
        // Test that generated identifiers would be valid Rust identifiers
        let valid_inputs = vec![
            "valid_rust_identifier",
            "another_valid_name",
            "test_123",
            "handler_v2",
            "process_data",
        ];

        for input in valid_inputs {
            let pascal = to_pascal_case(input);
            let wrapper = format!("{pascal}Task");

            // Basic validation that it starts with letter and contains valid chars
            assert!(
                !wrapper.is_empty(),
                "Wrapper name should not be empty for {input}"
            );
            assert!(
                wrapper.chars().next().unwrap().is_alphabetic(),
                "Wrapper name should start with letter: {wrapper}"
            );
            assert!(
                wrapper.chars().all(|c| c.is_alphanumeric()),
                "Wrapper name should only contain alphanumeric chars: {wrapper}"
            );
        }
    }
}
