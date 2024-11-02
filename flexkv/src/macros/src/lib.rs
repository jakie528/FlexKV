mod expand;
mod since;
pub(crate) mod utils;

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use since::Since;
use syn::parse2;
use syn::parse_macro_input;
use syn::parse_str;
use syn::token::RArrow;
use syn::Item;
use syn::ReturnType;
use syn::TraitItem;
use syn::Type;

#[proc_macro_attribute]
pub fn add_async_trait(_attr: TokenStream, item: TokenStream) -> TokenStream {
        add_send_bounds(item)
}

/*
fn allow_non_send_bounds(item: TokenStream) -> TokenStream {
    let item: proc_macro2::TokenStream = item.into();
    quote! {
        #[allow(async_fn_in_trait)]
        #item
    }
    .into()
}
*/

fn add_send_bounds(item: TokenStream) -> TokenStream {
    let send_bound = parse_str("Send").unwrap();
    let default_return_type: Box<Type> = parse_str("impl std::future::Future<Output = ()> + Send").unwrap();

    match parse_macro_input!(item) {
        Item::Trait(mut input) => {
            input.supertraits.push(send_bound);

            for item in input.items.iter_mut() {
                let TraitItem::Fn(function) = item else { continue };
                if function.sig.asyncness.is_none() {
                    continue;
                };

                function.sig.asyncness = None;

                function.sig.output = match &function.sig.output {
                    ReturnType::Default => ReturnType::Type(RArrow::default(), default_return_type.clone()),
                    ReturnType::Type(arrow, t) => {
                        let tokens = quote!(impl std::future::Future<Output = #t> + Send);
                        ReturnType::Type(*arrow, parse2(tokens).unwrap())
                    }
                };

                let Some(body) = &function.default else { continue };
                let body = parse2(quote!({ async move #body })).unwrap();
                function.default = Some(body);
            }

            quote!(#input).into()
        }

        _ => panic!("add_async_trait can only be used with traits"),
    }
}

#[proc_macro_attribute]
pub fn since(args: TokenStream, item: TokenStream) -> TokenStream {
    let tokens = do_since(args, item.clone());
    match tokens {
        Ok(x) => x,
        Err(e) => utils::token_stream_with_error(item, e),
    }
}

fn do_since(args: TokenStream, item: TokenStream) -> Result<TokenStream, syn::Error> {
    let since = Since::new(args)?;
    let tokens = since.append_since_doc(item)?;
    Ok(tokens)
}

#[proc_macro]
pub fn expand(item: TokenStream) -> TokenStream {
    let repeat = parse_macro_input!(item as expand::Expand);
    repeat.render().into()
}
