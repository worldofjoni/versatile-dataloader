# Async Dataloader
A dataloader (originally developed by [facebook](https://github.com/graphql/dataloader)) allows to batch requests coming in at roughly the same time.
This is useful e.g., to avoid the [n+1 problem](https://async-graphql.github.io/async-graphql/en/dataloader.html) in GraphQL.

Forked/extracted from [async-graphql](https://github.com/async-graphql/async-graphql) as it has no dependencies towards async-graphql and might be useful on its own.

## Example
```rust
use std::collections::{HashSet, HashMap};
use std::convert::Infallible;
use async_dataloader::{Loader, DataLoader};

/// This loader simply converts the integer key into a string value.
struct MyLoader;

impl Loader<i32, String> for MyLoader {
    type Error = Infallible;

    async fn load(&self, keys: &[i32]) -> Result<HashMap<i32, String>, Self::Error> {
        // Implement database access or similar here.
        Ok(keys.iter().copied().map(|n| (n, n.to_string())).collect())
    }
}


tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {

// Load data with dataloader:
let loader = DataLoader::new(MyLoader, tokio::spawn);
let strings = vec![loader.load_one(1).await.unwrap(), loader.load_one(2).await.unwrap(), loader.load_one(3).await.unwrap()];
// The dataloader load function is only called once.

assert_eq!(strings, vec![Some("1".to_string()), Some("2".to_string()), Some("3".to_string())]);

});
```

## Related projects
- [async-graphql](https://github.com/async-graphql/async-graphql) includes a dataloder where this one is based on. We adapted the API to make the return type generic rather than an associated type. This allows to use one struct as a dataloader for different return types all with the same key (e.g., a UUID). 
- [dataloader-rs](https://github.com/cksac/dataloader-rs) is similar, but it has a less versatile API that does not support errors.

## License

Licensed under either of

- Apache License, Version 2.0,
  ([LICENSE-APACHE](./LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](./LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
  at your option.