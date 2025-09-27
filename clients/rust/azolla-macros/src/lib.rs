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
