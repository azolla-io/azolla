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
    
    // Determine crate path - check for internal feature flag
    let crate_path = if cfg!(feature = "__azolla_internal__") {
        quote! { crate }
    } else {
        quote! { ::azolla_client }
    };

    // Extract parameter types and names
    let mut param_extractions = Vec::new();
    let mut param_names = Vec::new();
    
    for (i, input) in input_fn.sig.inputs.iter().enumerate() {
        if let FnArg::Typed(pat_type) = input {
            if let Pat::Ident(pat_ident) = pat_type.pat.as_ref() {
                let param_name = &pat_ident.ident;
                let param_type = &pat_type.ty;
                param_names.push(param_name);
                
                let extraction = quote! {
                    let #param_name: #param_type = #crate_path::convert::FromJsonValue::try_from(
                        args_iter.next()
                            .ok_or_else(|| #crate_path::error::TaskError::invalid_args(&format!("Missing argument {} ({})", #i, stringify!(#param_name))))?
                    ).map_err(|e| #crate_path::error::TaskError::invalid_args(&format!("Invalid type for argument {} ({}): {}", #i, stringify!(#param_name), e)))?;
                };
                param_extractions.push(extraction);
            }
        }
    }
    
    // Generate wrapper struct name (convert to PascalCase)
    let fn_name_pascal = to_pascal_case(&fn_name.to_string());
    let wrapper_struct_name = syn::Ident::new(&format!("{}Task", fn_name_pascal), fn_name.span());
    
    let expanded = quote! {
        // Keep the original function
        #input_fn
        
        // Generate wrapper struct
        #fn_vis struct #wrapper_struct_name;
        
        impl #crate_path::task::Task for #wrapper_struct_name {
            fn name(&self) -> &'static str {
                #fn_name_str
            }
            
            fn execute(&self, args: Vec<serde_json::Value>) -> std::pin::Pin<Box<dyn std::future::Future<Output = #crate_path::task::TaskResult> + Send + '_>> {
                Box::pin(async move {
                    // Use an iterator to consume the args vector and avoid cloning
                    let mut args_iter = args.into_iter();
                    
                    // Extract typed arguments
                    #(#param_extractions)*
                    
                    // Check for extra arguments
                    if args_iter.next().is_some() {
                        return Err(#crate_path::error::TaskError::invalid_args(
                            "Too many arguments provided"
                        ));
                    }
                    
                    // Call original function
                    let result = #fn_name(#(#param_names),*).await;
                    
                    // Convert result to JSON
                    match result {
                        Ok(value) => {
                            let json_value = serde_json::to_value(value)
                                .map_err(|e| #crate_path::error::TaskError::execution_failed(&format!("Failed to serialize result: {}", e)))?;
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