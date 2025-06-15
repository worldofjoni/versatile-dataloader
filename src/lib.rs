#![doc = include_str!("../README.md")]
#![warn(clippy::pedantic)]
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

mod cache;

#[cfg(not(feature = "boxed-trait"))]
use std::future::Future;
use std::{
    any::{Any, TypeId},
    borrow::Cow,
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

pub use cache::{CacheFactory, CacheStorage, HashMapCache, LruCache, NoCache};
use fnv::FnvHashMap;
use futures_channel::oneshot;
use futures_timer::Delay;
use futures_util::future::BoxFuture;
#[cfg(feature = "tracing")]
use tracing::{Instrument, info_span, instrument};
#[cfg(feature = "tracing")]
use tracinglib as tracing;

#[allow(clippy::type_complexity)]
struct ResSender<
    K: Send + Sync + Hash + Eq + Clone + 'static,
    V: Send + Sync + Clone + 'static,
    T: Loader<K, V>,
> {
    use_cache_values: HashMap<K, V>,
    tx: oneshot::Sender<Result<HashMap<K, V>, T::Error>>,
}

struct Requests<
    K: Send + Sync + Hash + Eq + Clone + 'static,
    V: Send + Sync + Clone + 'static,
    T: Loader<K, V>,
> {
    keys: HashSet<K>,
    pending: Vec<(HashSet<K>, ResSender<K, V, T>)>,
    cache_storage: Box<dyn CacheStorage<Key = K, Value = V>>,
    disable_cache: bool,
}

type KeysAndSender<K, V, T> = (HashSet<K>, Vec<(HashSet<K>, ResSender<K, V, T>)>);

impl<
    K: Send + Sync + Hash + Eq + Clone + 'static,
    V: Send + Sync + Clone + 'static,
    T: Loader<K, V>,
> Requests<K, V, T>
{
    fn new<C: CacheFactory>(cache_factory: &C) -> Self {
        Self {
            keys: HashSet::default(),
            pending: Vec::new(),
            cache_storage: cache_factory.create::<K, V>(),
            disable_cache: false,
        }
    }

    fn take(&mut self) -> KeysAndSender<K, V, T> {
        (
            std::mem::take(&mut self.keys),
            std::mem::take(&mut self.pending),
        )
    }
}

/// Trait for batch loading.
#[cfg_attr(feature = "boxed-trait", async_trait::async_trait)]
pub trait Loader<K: Send + Sync + Hash + Eq + Clone + 'static, V: Send + Sync + Clone + 'static>:
    Send + Sync + 'static
{
    /// Type of error.
    type Error: Send + Clone + 'static;

    /// Load the data set specified by the `keys`.
    #[cfg(feature = "boxed-trait")]
    async fn load(&self, keys: &[K]) -> Result<HashMap<K, V>, Self::Error>;

    /// Load the data set specified by the `keys`.
    #[cfg(not(feature = "boxed-trait"))]
    fn load(&self, keys: &[K]) -> impl Future<Output = Result<HashMap<K, V>, Self::Error>> + Send;
}

struct DataLoaderInner<T> {
    requests: Mutex<FnvHashMap<TypeId, Box<dyn Any + Sync + Send>>>,
    loader: T,
}

impl<T> DataLoaderInner<T> {
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    async fn do_load<K, V>(&self, disable_cache: bool, (keys, senders): KeysAndSender<K, V, T>)
    where
        K: Send + Sync + Hash + Eq + Clone + 'static,
        V: Send + Sync + Clone + 'static,
        T: Loader<K, V>,
    {
        let tid = TypeId::of::<(K, V)>();
        let keys = keys.into_iter().collect::<Vec<_>>();

        match self.loader.load(&keys).await {
            Ok(values) => {
                // update cache
                let mut request = self.requests.lock().unwrap();
                let typed_requests = request
                    .get_mut(&tid)
                    .unwrap()
                    .downcast_mut::<Requests<K, V, T>>()
                    .unwrap();
                let disable_cache = typed_requests.disable_cache || disable_cache;
                if !disable_cache {
                    for (key, value) in &values {
                        typed_requests
                            .cache_storage
                            .insert(Cow::Borrowed(key), Cow::Borrowed(value));
                    }
                }

                // send response
                for (keys, sender) in senders {
                    let mut res = HashMap::new();
                    res.extend(sender.use_cache_values);
                    for key in &keys {
                        res.extend(values.get(key).map(|value| (key.clone(), value.clone())));
                    }
                    sender.tx.send(Ok(res)).ok();
                }
            }
            Err(err) => {
                for (_, sender) in senders {
                    sender.tx.send(Err(err.clone())).ok();
                }
            }
        }
    }
}

/// Data loader.
///
/// Reference: <https://github.com/facebook/dataloader>
pub struct DataLoader<T, C = NoCache> {
    inner: Arc<DataLoaderInner<T>>,
    cache_factory: C,
    delay: Duration,
    max_batch_size: usize,
    disable_cache: AtomicBool,
    spawner: Box<dyn Fn(BoxFuture<'static, ()>) + Send + Sync>,
}

impl<T> DataLoader<T, NoCache> {
    /// Use `Loader` to create a [`DataLoader`] that does not cache records.
    pub fn new<S, R>(loader: T, spawner: S) -> Self
    where
        S: Fn(BoxFuture<'static, ()>) -> R + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(DataLoaderInner {
                requests: Mutex::new(HashMap::default()),
                loader,
            }),
            cache_factory: NoCache,
            delay: Duration::from_millis(1),
            max_batch_size: 1000,
            disable_cache: false.into(),
            spawner: Box::new(move |fut| {
                spawner(fut);
            }),
        }
    }
}

impl<T, C: CacheFactory> DataLoader<T, C> {
    /// Use `Loader` to create a [`DataLoader`] with a cache factory.
    pub fn with_cache<S, R>(loader: T, spawner: S, cache_factory: C) -> Self
    where
        S: Fn(BoxFuture<'static, ()>) -> R + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(DataLoaderInner {
                requests: Mutex::new(HashMap::default()),
                loader,
            }),
            cache_factory,
            delay: Duration::from_millis(1),
            max_batch_size: 1000,
            disable_cache: false.into(),
            spawner: Box::new(move |fut| {
                spawner(fut);
            }),
        }
    }

    /// Specify the delay time for loading data, the default is `1ms`.
    #[must_use]
    pub fn delay(self, delay: Duration) -> Self {
        Self { delay, ..self }
    }

    /// pub fn Specify the max batch size for loading data, the default is
    /// `1000`.
    ///
    /// If the keys waiting to be loaded reach the threshold, they are loaded
    /// immediately.
    #[must_use]
    pub fn max_batch_size(self, max_batch_size: usize) -> Self {
        Self {
            max_batch_size,
            ..self
        }
    }

    /// Get the loader.
    #[inline]
    pub fn loader(&self) -> &T {
        &self.inner.loader
    }

    /// Enable/Disable cache of all loaders.
    pub fn enable_all_cache(&self, enable: bool) {
        self.disable_cache.store(!enable, Ordering::SeqCst);
    }

    /// Enable/Disable cache of specified loader.
    pub fn enable_cache<K, V>(&self, enable: bool)
    where
        K: Send + Sync + Hash + Eq + Clone + 'static,
        V: Send + Sync + Clone + 'static,
        T: Loader<K, V>,
    {
        let tid = TypeId::of::<(K, V)>();
        let mut requests = self.inner.requests.lock().unwrap();
        let typed_requests = requests
            .get_mut(&tid)
            .unwrap()
            .downcast_mut::<Requests<K, V, T>>()
            .unwrap();
        typed_requests.disable_cache = !enable;
    }

    /// Use this `DataLoader` load a data.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    pub async fn load_one<K, V>(&self, key: K) -> Result<Option<V>, T::Error>
    where
        K: Send + Sync + Hash + Eq + Clone + 'static,
        V: Send + Sync + Clone + 'static,
        T: Loader<K, V>,
    {
        let mut values = self.load_many(std::iter::once(key.clone())).await?;
        Ok(values.remove(&key))
    }

    /// Use this `DataLoader` to load some data.
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    pub async fn load_many<K, V, I>(&self, keys: I) -> Result<HashMap<K, V>, T::Error>
    where
        K: Send + Sync + Hash + Eq + Clone + 'static,
        I: IntoIterator<Item = K>,
        V: Send + Sync + Clone + 'static,
        T: Loader<K, V>,
    {
        enum Action<
            K: Send + Sync + Hash + Eq + Clone + 'static,
            V: Send + Sync + Clone + 'static,
            T: Loader<K, V>,
        > {
            ImmediateLoad(KeysAndSender<K, V, T>),
            StartFetch,
            Delay,
        }

        let tid = TypeId::of::<(K, V)>();

        let (action, rx) = {
            let mut requests = self.inner.requests.lock().unwrap();
            let typed_requests = requests
                .entry(tid)
                .or_insert_with(|| Box::new(Requests::<K, V, T>::new(&self.cache_factory)))
                .downcast_mut::<Requests<K, V, T>>()
                .unwrap();
            let prev_count = typed_requests.keys.len();
            let mut keys_set = HashSet::new();
            let mut use_cache_values = HashMap::new();

            if typed_requests.disable_cache || self.disable_cache.load(Ordering::SeqCst) {
                keys_set = keys.into_iter().collect();
            } else {
                for key in keys {
                    if let Some(value) = typed_requests.cache_storage.get(&key) {
                        // Already in cache
                        use_cache_values.insert(key.clone(), value.clone());
                    } else {
                        keys_set.insert(key);
                    }
                }
            }

            if !use_cache_values.is_empty() && keys_set.is_empty() {
                return Ok(use_cache_values);
            } else if use_cache_values.is_empty() && keys_set.is_empty() {
                return Ok(HashMap::default());
            }

            typed_requests.keys.extend(keys_set.clone());
            let (tx, rx) = oneshot::channel();
            typed_requests.pending.push((
                keys_set,
                ResSender {
                    use_cache_values,
                    tx,
                },
            ));

            if typed_requests.keys.len() >= self.max_batch_size {
                (Action::ImmediateLoad(typed_requests.take()), rx)
            } else {
                (
                    if !typed_requests.keys.is_empty() && prev_count == 0 {
                        Action::StartFetch
                    } else {
                        Action::Delay
                    },
                    rx,
                )
            }
        };

        match action {
            Action::ImmediateLoad(keys) => {
                let inner = self.inner.clone();
                let disable_cache = self.disable_cache.load(Ordering::SeqCst);
                let task = async move { inner.do_load(disable_cache, keys).await };
                #[cfg(feature = "tracing")]
                let task = task
                    .instrument(info_span!("immediate_load"))
                    .in_current_span();

                (self.spawner)(Box::pin(task));
            }
            Action::StartFetch => {
                let inner = self.inner.clone();
                let disable_cache = self.disable_cache.load(Ordering::SeqCst);
                let delay = self.delay;

                let task = async move {
                    Delay::new(delay).await;

                    let keys = {
                        let mut request = inner.requests.lock().unwrap();
                        let typed_requests = request
                            .get_mut(&tid)
                            .unwrap()
                            .downcast_mut::<Requests<K, V, T>>()
                            .unwrap();
                        typed_requests.take()
                    };

                    if !keys.0.is_empty() {
                        inner.do_load(disable_cache, keys).await;
                    }
                };
                #[cfg(feature = "tracing")]
                let task = task.instrument(info_span!("start_fetch")).in_current_span();
                (self.spawner)(Box::pin(task));
            }
            Action::Delay => {}
        }

        rx.await.unwrap()
    }

    /// Feed some data into the cache.
    ///
    /// **NOTE: If the cache type is [`NoCache`], this function will not take
    /// effect. **
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    #[allow(clippy::unused_async)]
    pub async fn feed_many<K, V, I>(&self, values: I)
    where
        K: Send + Sync + Hash + Eq + Clone + 'static,
        I: IntoIterator<Item = (K, V)>,
        V: Send + Sync + Clone + 'static,
        T: Loader<K, V>,
    {
        let tid = TypeId::of::<(K, V)>();
        let mut requests = self.inner.requests.lock().unwrap();
        let typed_requests = requests
            .entry(tid)
            .or_insert_with(|| Box::new(Requests::<K, V, T>::new(&self.cache_factory)))
            .downcast_mut::<Requests<K, V, T>>()
            .unwrap();
        for (key, value) in values {
            typed_requests
                .cache_storage
                .insert(Cow::Owned(key), Cow::Owned(value));
        }
    }

    /// Feed some data into the cache.
    ///
    /// **NOTE: If the cache type is [`NoCache`], this function will not take
    /// effect. **
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    pub async fn feed_one<K, V>(&self, key: K, value: V)
    where
        K: Send + Sync + Hash + Eq + Clone + 'static,
        V: Send + Sync + Clone + 'static,
        T: Loader<K, V>,
    {
        self.feed_many(std::iter::once((key, value))).await;
    }

    /// Clears the cache.
    ///
    /// **NOTE: If the cache type is [`NoCache`], this function will not take
    /// effect. **
    #[cfg_attr(feature = "tracing", instrument(skip_all))]
    pub fn clear<K, V>(&self)
    where
        K: Send + Sync + Hash + Eq + Clone + 'static,
        V: Send + Sync + Clone + 'static,
        T: Loader<K, V>,
    {
        let tid = TypeId::of::<(K, V)>();
        let mut requests = self.inner.requests.lock().unwrap();
        let typed_requests = requests
            .entry(tid)
            .or_insert_with(|| Box::new(Requests::<K, V, T>::new(&self.cache_factory)))
            .downcast_mut::<Requests<K, V, T>>()
            .unwrap();
        typed_requests.cache_storage.clear();
    }

    /// Gets all values in the cache.
    pub fn get_cached_values<K, V>(&self) -> HashMap<K, V>
    where
        K: Send + Sync + Hash + Eq + Clone + 'static,
        V: Send + Sync + Clone + 'static,
        T: Loader<K, V>,
    {
        let tid = TypeId::of::<(K, V)>();
        let requests = self.inner.requests.lock().unwrap();
        match requests.get(&tid) {
            None => HashMap::new(),
            Some(requests) => {
                let typed_requests = requests.downcast_ref::<Requests<K, V, T>>().unwrap();
                typed_requests
                    .cache_storage
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use fnv::FnvBuildHasher;
    use tokio::join;

    use super::*;

    struct MyLoader;

    #[cfg_attr(feature = "boxed-trait", async_trait::async_trait)]
    impl Loader<i32, i32> for MyLoader {
        type Error = ();

        async fn load(&self, keys: &[i32]) -> Result<HashMap<i32, i32>, Self::Error> {
            assert!(keys.len() <= 10);
            Ok(keys.iter().copied().map(|k| (k, k)).collect())
        }
    }

    #[cfg_attr(feature = "boxed-trait", async_trait::async_trait)]
    impl Loader<i64, i64> for MyLoader {
        type Error = ();

        async fn load(&self, keys: &[i64]) -> Result<HashMap<i64, i64>, Self::Error> {
            assert!(keys.len() <= 10);
            Ok(keys.iter().copied().map(|k| (k, k)).collect())
        }
    }

    #[tokio::test]
    async fn test_dataloader() {
        let loader = Arc::new(DataLoader::new(MyLoader, tokio::spawn).max_batch_size(10));
        assert_eq!(
            futures_util::future::try_join_all((0..100i32).map({
                let loader = loader.clone();
                move |n| {
                    let loader = loader.clone();
                    async move { loader.load_one(n).await }
                }
            }))
            .await
            .unwrap(),
            (0..100).map(Option::Some).collect::<Vec<_>>()
        );

        assert_eq!(
            futures_util::future::try_join_all((0..100i64).map({
                let loader = loader.clone();
                move |n| {
                    let loader = loader.clone();
                    async move { loader.load_one(n).await }
                }
            }))
            .await
            .unwrap(),
            (0..100).map(Option::Some).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn test_duplicate_keys() {
        let loader = Arc::new(DataLoader::new(MyLoader, tokio::spawn).max_batch_size(10));
        assert_eq!(
            futures_util::future::try_join_all([1, 3, 5, 1, 7, 8, 3, 7].iter().copied().map({
                let loader = loader.clone();
                move |n| {
                    let loader = loader.clone();
                    async move { loader.load_one(n).await }
                }
            }))
            .await
            .unwrap(),
            [1, 3, 5, 1, 7, 8, 3, 7]
                .iter()
                .copied()
                .map(Option::Some)
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn test_dataloader_load_empty() {
        let loader = DataLoader::new(MyLoader, tokio::spawn);
        assert!(
            loader
                .load_many::<i32, _, _>(vec![])
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_dataloader_with_cache() {
        let loader = DataLoader::with_cache(MyLoader, tokio::spawn, HashMapCache::default());
        loader.feed_many(vec![(1, 10), (2, 20), (3, 30)]).await;

        // All from the cache
        assert_eq!(
            loader.load_many(vec![1, 2, 3]).await.unwrap(),
            vec![(1, 10), (2, 20), (3, 30)].into_iter().collect()
        );

        // Part from the cache
        assert_eq!(
            loader.load_many(vec![1, 5, 6]).await.unwrap(),
            vec![(1, 10), (5, 5), (6, 6)].into_iter().collect()
        );

        // All from the loader
        assert_eq!(
            loader.load_many(vec![8, 9, 10]).await.unwrap(),
            vec![(8, 8), (9, 9), (10, 10)].into_iter().collect()
        );

        // Clear cache
        loader.clear::<i32, _>();
        assert_eq!(
            loader.load_many(vec![1, 2, 3]).await.unwrap(),
            vec![(1, 1), (2, 2), (3, 3)].into_iter().collect()
        );
    }

    #[tokio::test]
    async fn test_dataloader_with_cache_hashmap_fnv() {
        let loader = DataLoader::with_cache(
            MyLoader,
            tokio::spawn,
            HashMapCache::<FnvBuildHasher>::new(),
        );
        loader.feed_many(vec![(1, 10), (2, 20), (3, 30)]).await;

        // All from the cache
        assert_eq!(
            loader.load_many(vec![1, 2, 3]).await.unwrap(),
            vec![(1, 10), (2, 20), (3, 30)].into_iter().collect()
        );

        // Part from the cache
        assert_eq!(
            loader.load_many(vec![1, 5, 6]).await.unwrap(),
            vec![(1, 10), (5, 5), (6, 6)].into_iter().collect()
        );

        // All from the loader
        assert_eq!(
            loader.load_many(vec![8, 9, 10]).await.unwrap(),
            vec![(8, 8), (9, 9), (10, 10)].into_iter().collect()
        );

        // Clear cache
        loader.clear::<i32, _>();
        assert_eq!(
            loader.load_many(vec![1, 2, 3]).await.unwrap(),
            vec![(1, 1), (2, 2), (3, 3)].into_iter().collect()
        );
    }

    #[tokio::test]
    async fn test_dataloader_disable_all_cache() {
        let loader = DataLoader::with_cache(MyLoader, tokio::spawn, HashMapCache::default());
        loader.feed_many(vec![(1, 10), (2, 20), (3, 30)]).await;

        // All from the loader
        loader.enable_all_cache(false);
        assert_eq!(
            loader.load_many(vec![1, 2, 3]).await.unwrap(),
            vec![(1, 1), (2, 2), (3, 3)].into_iter().collect()
        );

        // All from the cache
        loader.enable_all_cache(true);
        assert_eq!(
            loader.load_many(vec![1, 2, 3]).await.unwrap(),
            vec![(1, 10), (2, 20), (3, 30)].into_iter().collect()
        );
    }

    #[tokio::test]
    async fn test_dataloader_disable_cache() {
        let loader = DataLoader::with_cache(MyLoader, tokio::spawn, HashMapCache::default());
        loader.feed_many(vec![(1, 10), (2, 20), (3, 30)]).await;

        // All from the loader
        loader.enable_cache::<i32, _>(false);
        assert_eq!(
            loader.load_many(vec![1, 2, 3]).await.unwrap(),
            vec![(1, 1), (2, 2), (3, 3)].into_iter().collect()
        );

        // All from the cache
        loader.enable_cache::<i32, _>(true);
        assert_eq!(
            loader.load_many(vec![1, 2, 3]).await.unwrap(),
            vec![(1, 10), (2, 20), (3, 30)].into_iter().collect()
        );
    }

    #[tokio::test]
    async fn test_dataloader_dead_lock() {
        struct MyDelayLoader;

        #[cfg_attr(feature = "boxed-trait", async_trait::async_trait)]
        impl Loader<i32, i32> for MyDelayLoader {
            type Error = ();

            async fn load(&self, keys: &[i32]) -> Result<HashMap<i32, i32>, Self::Error> {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(keys.iter().copied().map(|k| (k, k)).collect())
            }
        }

        let loader = Arc::new(
            DataLoader::with_cache(MyDelayLoader, tokio::spawn, NoCache)
                .delay(Duration::from_secs(1)),
        );
        let handle = tokio::spawn({
            let loader = loader.clone();
            async move {
                loader.load_many(vec![1, 2, 3]).await.unwrap();
            }
        });

        tokio::time::sleep(Duration::from_millis(500)).await;
        handle.abort();
        loader.load_many(vec![4, 5, 6]).await.unwrap();
    }

    #[tokio::test]
    async fn test_load_different_keys() {
        struct MyDelayLoader;
        #[cfg_attr(feature = "boxed-trait", async_trait::async_trait)]
        impl Loader<i32, i32> for MyDelayLoader {
            type Error = ();

            async fn load(&self, keys: &[i32]) -> Result<HashMap<i32, i32>, Self::Error> {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(keys.iter().copied().map(|k| (k, k)).collect())
            }
        }
        #[cfg_attr(feature = "boxed-trait", async_trait::async_trait)]
        impl Loader<i32, u32> for MyDelayLoader {
            type Error = ();

            async fn load(&self, keys: &[i32]) -> Result<HashMap<i32, u32>, Self::Error> {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(keys
                    .iter()
                    .copied()
                    .map(|k| (k, k.try_into().unwrap()))
                    .collect())
            }
        }

        let loader = DataLoader::new(MyDelayLoader, tokio::spawn).delay(Duration::from_secs(1));

        let x = join!(loader.load_one(1), loader.load_one(1));
        let x1: u32 = x.0.unwrap().unwrap();
        let x2: i32 = x.1.unwrap().unwrap();
        assert_eq!(x2, 1i32);
        assert_eq!(x1, 1u32);
    }
}
