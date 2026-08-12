//! API for downloads from multiple nodes.
use std::{
    collections::HashMap,
    fmt::Debug,
    future::{Future, IntoFuture},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use genawaiter::sync::Gen;
use iroh::{Endpoint, EndpointId};
use irpc::{
    channel::{mpsc, oneshot},
    rpc_requests,
};
use n0_error::{anyerr, Result};
use n0_future::{
    future, stream,
    task::{JoinError, JoinSet},
    BufferedStreamExt, Stream, StreamExt,
};
use rand::seq::SliceRandom;
use serde::{de::Error, Deserialize, Serialize};
use tracing::instrument::Instrument;

use super::Store;
use crate::{
    protocol::{GetManyRequest, GetRequest},
    util::{
        connection_pool::ConnectionPool,
        sink::{IrpcSenderRefSink, Sink, TokioMpscSenderSink},
    },
    BlobFormat, Hash, HashAndFormat,
};

#[derive(Debug, Clone)]
pub struct Downloader {
    client: irpc::Client<SwarmProtocol>,
}

#[rpc_requests(message = SwarmMsg, alias = "Msg", rpc_feature = "rpc")]
#[derive(Debug, Serialize, Deserialize)]
enum SwarmProtocol {
    #[rpc(tx = mpsc::Sender<DownloadProgressItem>)]
    Download(DownloadRequest),
    #[rpc(tx = oneshot::Sender<()>)]
    WaitIdle(WaitIdleRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WaitIdleRequest;

struct DownloaderActor {
    store: Store,
    pool: ConnectionPool,
    tasks: JoinSet<()>,
    idle_waiters: Vec<irpc::channel::oneshot::Sender<()>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DownloadProgressItem {
    #[serde(skip)]
    Error(n0_error::AnyError),
    TryProvider {
        id: EndpointId,
        request: Arc<GetRequest>,
    },
    ProviderFailed {
        id: EndpointId,
        request: Arc<GetRequest>,
    },
    PartComplete {
        request: Arc<GetRequest>,
    },
    Progress(u64),
    /// Payload bytes actually read from a provider during one attempt, emitted
    /// once when that attempt ends (before its [`Self::PartComplete`] or
    /// [`Self::ProviderFailed`]), and only when non-zero.
    ///
    /// Unlike [`Self::Progress`], which is a cumulative offset that includes
    /// bytes already resident locally, these values are additive: summing them
    /// over a download gives the exact payload transferred, including bytes
    /// received before an attempt failed.
    BytesTransferred(u64),
    DownloadError,
}

impl DownloaderActor {
    fn new_with_opts(
        store: Store,
        endpoint: Endpoint,
        pool_options: crate::util::connection_pool::Options,
    ) -> Self {
        Self {
            store,
            pool: ConnectionPool::new(endpoint, crate::ALPN, pool_options),
            tasks: JoinSet::new(),
            idle_waiters: Vec::new(),
        }
    }

    async fn run(mut self, mut rx: tokio::sync::mpsc::Receiver<SwarmMsg>) {
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    let Some(msg) = msg else { break };
                    match msg {
                        SwarmMsg::Download(request) => {
                            self.spawn(handle_download(
                                self.store.clone(),
                                self.pool.clone(),
                                request,
                            ));
                        }
                        SwarmMsg::WaitIdle(WaitIdleMsg { tx, .. }) => {
                            if self.tasks.is_empty() {
                                tx.send(()).await.ok();
                            } else {
                                self.idle_waiters.push(tx);
                            }
                        }
                    }
                }
                Some(res) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    Self::log_task_result(res);
                    if self.tasks.is_empty() {
                        for tx in self.idle_waiters.drain(..) {
                            tx.send(()).await.ok();
                        }
                    }
                }
            }
        }
        while let Some(res) = self.tasks.join_next().await {
            Self::log_task_result(res);
        }
    }

    fn log_task_result(res: std::result::Result<(), JoinError>) {
        match res {
            Ok(()) => {}
            Err(e) if e.is_cancelled() => tracing::trace!("download task cancelled: {e}"),
            Err(e) => tracing::error!("download task failed: {e}"),
        }
    }

    fn spawn(&mut self, fut: impl Future<Output = ()> + Send + 'static) {
        let span = tracing::Span::current();
        self.tasks.spawn(fut.instrument(span));
    }
}

async fn handle_download(store: Store, pool: ConnectionPool, msg: DownloadMsg) {
    let DownloadMsg { inner, mut tx, .. } = msg;
    if let Err(cause) = handle_download_impl(store, pool, inner, &mut tx).await {
        tx.send(DownloadProgressItem::Error(cause)).await.ok();
    }
}

async fn handle_download_impl(
    store: Store,
    pool: ConnectionPool,
    request: DownloadRequest,
    tx: &mut mpsc::Sender<DownloadProgressItem>,
) -> Result<()> {
    match request.strategy {
        SplitStrategy::Split => handle_download_split_impl(store, pool, request, tx).await?,
        SplitStrategy::None => match request.request {
            FiniteRequest::Get(get) => {
                let sink = IrpcSenderRefSink(tx);
                execute_get(&pool, Arc::new(get), &request.providers, &store, sink).await?;
            }
            FiniteRequest::GetMany(_) => {
                handle_download_split_impl(store, pool, request, tx).await?
            }
        },
    }
    Ok(())
}

async fn handle_download_split_impl(
    store: Store,
    pool: ConnectionPool,
    request: DownloadRequest,
    tx: &mut mpsc::Sender<DownloadProgressItem>,
) -> Result<()> {
    let providers = request.providers;
    // The root fetch is a real provider attempt: report it like any other,
    // instead of dropping its events into a `Drain`.
    let requests = split_request(
        &request.request,
        &providers,
        &pool,
        &store,
        IrpcSenderRefSink(&mut *tx),
    )
    .await?;
    let (progress_tx, progress_rx) = tokio::sync::mpsc::channel(32);
    let mut futs = stream::iter(requests.into_iter().enumerate())
        .map(|(id, request)| {
            let pool = pool.clone();
            let providers = providers.clone();
            let store = store.clone();
            let progress_tx = progress_tx.clone();
            async move {
                let hash = request.hash;
                let (tx, rx) = tokio::sync::mpsc::channel::<(usize, DownloadProgressItem)>(16);
                progress_tx.send(rx).await.ok();
                let sink = TokioMpscSenderSink(tx).with_map(move |x| (id, x));
                let res = execute_get(&pool, Arc::new(request), &providers, &store, sink).await;
                (hash, res)
            }
        })
        .buffered_unordered(32);
    let mut progress_stream = {
        let mut offsets = HashMap::new();
        let mut total = 0;
        into_stream(progress_rx)
            .flat_map(into_stream)
            .map(move |(id, item)| match item {
                DownloadProgressItem::Progress(offset) => {
                    total += offset;
                    if let Some(prev) = offsets.insert(id, offset) {
                        total -= prev;
                    }
                    DownloadProgressItem::Progress(total)
                }
                x => x,
            })
    };
    let mut closed = false;
    loop {
        tokio::select! {
            Some(item) = progress_stream.next() => {
                tx.send(item).await?;
            },
            res = futs.next() => {
                match res {
                    Some((_hash, Ok(()))) => {
                    }
                    Some((_hash, Err(_e))) => {
                        tx.send(DownloadProgressItem::DownloadError).await?;
                    }
                    None => break,
                }
            }
            _ = tx.closed() => {
                // The sender has been closed, we should stop processing.
                closed = true;
                break;
            }
        }
    }
    if closed {
        return Ok(());
    }
    // Every child has finished, but its last events can still be sitting in the
    // fan-in. Drop the futures stream and the producer handle so the fan-in can
    // terminate, then forward what is left: otherwise a final `TryProvider`,
    // `BytesTransferred`, `ProviderFailed` or `PartComplete` is lost whenever a
    // child completes fast enough to end `futs` first.
    drop(futs);
    drop(progress_tx);
    while let Some(item) = progress_stream.next().await {
        tx.send(item).await?;
    }
    Ok(())
}

fn into_stream<T>(mut recv: tokio::sync::mpsc::Receiver<T>) -> impl Stream<Item = T> {
    Gen::new(|co| async move {
        while let Some(item) = recv.recv().await {
            co.yield_(item).await;
        }
    })
}

#[derive(Debug, Serialize, Deserialize, derive_more::From)]
pub enum FiniteRequest {
    Get(GetRequest),
    GetMany(GetManyRequest),
}

pub trait SupportedRequest {
    fn into_request(self) -> FiniteRequest;
}

impl<I: Into<Hash>, T: IntoIterator<Item = I>> SupportedRequest for T {
    fn into_request(self) -> FiniteRequest {
        let hashes = self.into_iter().map(Into::into).collect::<GetManyRequest>();
        FiniteRequest::GetMany(hashes)
    }
}

impl SupportedRequest for GetRequest {
    fn into_request(self) -> FiniteRequest {
        self.into()
    }
}

impl SupportedRequest for GetManyRequest {
    fn into_request(self) -> FiniteRequest {
        self.into()
    }
}

impl SupportedRequest for Hash {
    fn into_request(self) -> FiniteRequest {
        GetRequest::blob(self).into()
    }
}

impl SupportedRequest for HashAndFormat {
    fn into_request(self) -> FiniteRequest {
        (match self.format {
            BlobFormat::Raw => GetRequest::blob(self.hash),
            BlobFormat::HashSeq => GetRequest::all(self.hash),
        })
        .into()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddProviderRequest {
    pub hash: Hash,
    pub providers: Vec<EndpointId>,
}

#[derive(Debug)]
pub struct DownloadRequest {
    pub request: FiniteRequest,
    pub providers: Arc<dyn ContentDiscovery>,
    pub strategy: SplitStrategy,
}

impl DownloadRequest {
    pub fn new(
        request: impl SupportedRequest,
        providers: impl ContentDiscovery,
        strategy: SplitStrategy,
    ) -> Self {
        Self {
            request: request.into_request(),
            providers: Arc::new(providers),
            strategy,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SplitStrategy {
    None,
    Split,
}

impl Serialize for DownloadRequest {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom(
            "cannot serialize DownloadRequest",
        ))
    }
}

// Implement Deserialize to always fail
impl<'de> Deserialize<'de> for DownloadRequest {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Err(D::Error::custom("cannot deserialize DownloadRequest"))
    }
}

pub type DownloadOptions = DownloadRequest;

pub struct DownloadProgress {
    fut: future::Boxed<irpc::Result<mpsc::Receiver<DownloadProgressItem>>>,
}

impl DownloadProgress {
    fn new(fut: future::Boxed<irpc::Result<mpsc::Receiver<DownloadProgressItem>>>) -> Self {
        Self { fut }
    }

    pub async fn stream(self) -> irpc::Result<impl Stream<Item = DownloadProgressItem> + Unpin> {
        let rx = self.fut.await?;
        Ok(Box::pin(rx.into_stream().map(|item| match item {
            Ok(item) => item,
            Err(e) => DownloadProgressItem::Error(e.into()),
        })))
    }

    async fn complete(self) -> Result<()> {
        let rx = self.fut.await?;
        let stream = rx.into_stream();
        tokio::pin!(stream);
        while let Some(item) = stream.next().await {
            match item? {
                DownloadProgressItem::Error(e) => Err(e)?,
                DownloadProgressItem::DownloadError => {
                    n0_error::bail_any!("Download error");
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl IntoFuture for DownloadProgress {
    type Output = Result<()>;
    type IntoFuture = future::Boxed<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.complete())
    }
}

impl Downloader {
    pub fn new(store: &Store, endpoint: &Endpoint) -> Self {
        Self::new_with_opts(store, endpoint, Default::default())
    }

    pub fn new_with_opts(
        store: &Store,
        endpoint: &Endpoint,
        pool_options: crate::util::connection_pool::Options,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel::<SwarmMsg>(32);
        let actor = DownloaderActor::new_with_opts(store.clone(), endpoint.clone(), pool_options);
        n0_future::task::spawn(actor.run(rx));
        Self { client: tx.into() }
    }

    pub fn download(
        &self,
        request: impl SupportedRequest,
        providers: impl ContentDiscovery,
    ) -> DownloadProgress {
        let request = request.into_request();
        let providers = Arc::new(providers);
        self.download_with_opts(DownloadOptions {
            request,
            providers,
            strategy: SplitStrategy::None,
        })
    }

    pub fn download_with_opts(&self, options: DownloadOptions) -> DownloadProgress {
        let fut = self.client.server_streaming(options, 32);
        DownloadProgress::new(Box::pin(fut))
    }

    /// Wait until the downloader has no in-flight download tasks.
    ///
    /// This is mostly useful for tests, where you want to confirm that all
    /// previously-issued downloads have settled (whether they completed
    /// successfully or errored out). Note that the downloader is not
    /// guaranteed to become idle if it is being interacted with concurrently;
    /// in that case this might wait forever. Also note that once you get the
    /// callback, the downloader is not guaranteed to still be idle — all this
    /// tells you is that there was a point in time, between the call and the
    /// response, where it was idle.
    pub async fn wait_idle(&self) -> irpc::Result<()> {
        self.client.rpc(WaitIdleRequest).await?;
        Ok(())
    }
}

/// Split a request into multiple requests that can be run in parallel.
async fn split_request<'a>(
    request: &'a FiniteRequest,
    providers: &Arc<dyn ContentDiscovery>,
    pool: &ConnectionPool,
    store: &Store,
    progress: impl Sink<DownloadProgressItem, Error = irpc::channel::SendError>,
) -> Result<Box<dyn Iterator<Item = GetRequest> + Send + 'a>> {
    Ok(match request {
        FiniteRequest::Get(req) => {
            let Some(_first) = req.ranges.iter_infinite().next() else {
                return Ok(Box::new(std::iter::empty()));
            };
            let first = GetRequest::blob(req.hash);
            execute_get(pool, Arc::new(first), providers, store, progress).await?;
            let size = store.observe(req.hash).await?.size();
            n0_error::ensure_any!(size % 32 == 0, "Size is not a multiple of 32");
            let n = size / 32;
            Box::new(
                req.ranges
                    .iter_infinite()
                    .take(n as usize + 1)
                    .enumerate()
                    .filter_map(|(i, ranges)| {
                        if i != 0 && !ranges.is_empty() {
                            Some(
                                GetRequest::builder()
                                    .offset(i as u64, ranges.clone())
                                    .build(req.hash),
                            )
                        } else {
                            None
                        }
                    }),
            )
        }
        FiniteRequest::GetMany(req) => Box::new(
            req.hashes
                .iter()
                .enumerate()
                .map(|(i, hash)| GetRequest::blob_ranges(*hash, req.ranges[i as u64].clone())),
        ),
    })
}

/// Emit the payload total for one provider attempt, skipping the zero case.
async fn send_bytes_transferred(
    mut progress: impl Sink<DownloadProgressItem, Error = irpc::channel::SendError>,
    bytes: u64,
) -> Result<()> {
    if bytes > 0 {
        progress
            .send(DownloadProgressItem::BytesTransferred(bytes))
            .await?;
    }
    Ok(())
}

/// Execute a get request sequentially for multiple providers.
///
/// It will try each provider in order
/// until it finds one that can fulfill the request. When trying a new provider,
/// it takes the progress from the previous providers into account, so e.g.
/// if the first provider had the first 10% of the data, it will only ask the next
/// provider for the remaining 90%.
///
/// This is fully sequential, so there will only be one request in flight at a time.
///
/// If the request is not complete after trying all providers, it will return an error.
/// If the provider stream never ends, it will try indefinitely.
async fn execute_get(
    pool: &ConnectionPool,
    request: Arc<GetRequest>,
    providers: &Arc<dyn ContentDiscovery>,
    store: &Store,
    mut progress: impl Sink<DownloadProgressItem, Error = irpc::channel::SendError>,
) -> Result<()> {
    let remote = store.remote();
    let mut providers = providers.find_providers(request.content());
    while let Some(provider) = providers.next().await {
        let conn = pool.get_or_connect(provider);
        let local = remote.local_for_request(request.clone()).await?;
        if local.is_complete() {
            return Ok(());
        }
        // Emitted only once we know there is something left to fetch, so
        // `TryProvider` means an actual transfer attempt rather than a no-op on
        // already-resident content.
        progress
            .send(DownloadProgressItem::TryProvider {
                id: provider,
                request: request.clone(),
            })
            .await?;
        let local_bytes = local.local_bytes();
        let Ok(conn) = conn.await else {
            progress
                .send(DownloadProgressItem::ProviderFailed {
                    id: provider,
                    request: request.clone(),
                })
                .await?;
            continue;
        };
        // Raw per-attempt payload counter, kept so a *failed* attempt can still
        // report the bytes it did transfer. On success we use the exact figure
        // from the returned stats instead of the last progress update.
        let attempt_bytes = Arc::new(AtomicU64::new(0));
        let attempt_counter = attempt_bytes.clone();
        match remote
            .execute_get_sink(
                conn.clone(),
                local.missing(),
                (&mut progress).with_map(move |x| {
                    attempt_counter.store(x, Ordering::Relaxed);
                    DownloadProgressItem::Progress(x + local_bytes)
                }),
            )
            .await
        {
            Ok(stats) => {
                send_bytes_transferred(&mut progress, stats.payload_bytes_read).await?;
                progress
                    .send(DownloadProgressItem::PartComplete {
                        request: request.clone(),
                    })
                    .await?;
                return Ok(());
            }
            Err(_cause) => {
                send_bytes_transferred(&mut progress, attempt_bytes.load(Ordering::Relaxed))
                    .await?;
                progress
                    .send(DownloadProgressItem::ProviderFailed {
                        id: provider,
                        request: request.clone(),
                    })
                    .await?;
                continue;
            }
        }
    }
    Err(anyerr!("Unable to download {}", request.hash))
}

/// Trait for pluggable content discovery strategies.
pub trait ContentDiscovery: Debug + Send + Sync + 'static {
    fn find_providers(&self, hash: HashAndFormat) -> n0_future::stream::Boxed<EndpointId>;
}

impl<C, I> ContentDiscovery for C
where
    C: Debug + Clone + IntoIterator<Item = I> + Send + Sync + 'static,
    C::IntoIter: Send + Sync + 'static,
    I: Into<EndpointId> + Send + Sync + 'static,
{
    fn find_providers(&self, _: HashAndFormat) -> n0_future::stream::Boxed<EndpointId> {
        let providers = self.clone();
        n0_future::stream::iter(providers.into_iter().map(Into::into)).boxed()
    }
}

#[derive(derive_more::Debug)]
pub struct Shuffled {
    nodes: Vec<EndpointId>,
}

impl Shuffled {
    pub fn new(nodes: Vec<EndpointId>) -> Self {
        Self { nodes }
    }
}

impl ContentDiscovery for Shuffled {
    fn find_providers(&self, _: HashAndFormat) -> n0_future::stream::Boxed<EndpointId> {
        let mut nodes = self.nodes.clone();
        nodes.shuffle(&mut rand::rng());
        n0_future::stream::iter(nodes).boxed()
    }
}

#[cfg(test)]
#[cfg(feature = "fs-store")]
mod tests {
    use std::ops::Deref;

    use bao_tree::ChunkRanges;
    use iroh::EndpointId;
    use n0_future::StreamExt;
    use testresult::TestResult;

    use crate::{
        api::{
            blobs::AddBytesOptions,
            downloader::{
                DownloadOptions, DownloadProgressItem, Downloader, Shuffled, SplitStrategy,
            },
        },
        hashseq::HashSeq,
        protocol::{GetManyRequest, GetRequest},
        tests::node_test_setup_fs,
        Hash,
    };

    #[tokio::test]
    #[ignore = "todo"]
    async fn downloader_get_many_smoke() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let (r2, store2, _, _) = node_test_setup_fs(testdir.path().join("b")).await?;
        let (r3, store3, _, sp3) = node_test_setup_fs(testdir.path().join("c")).await?;
        let tt1 = store1.add_slice("hello world").await?;
        let tt2 = store2.add_slice("hello world 2").await?;
        let node1_addr = r1.endpoint().addr();
        let node1_id = node1_addr.id;
        let node2_addr = r2.endpoint().addr();
        let node2_id = node2_addr.id;
        let swarm = Downloader::new(&store3, r3.endpoint());
        sp3.add_endpoint_info(node1_addr.clone());
        sp3.add_endpoint_info(node2_addr.clone());
        let request = GetManyRequest::builder()
            .hash(tt1.hash, ChunkRanges::all())
            .hash(tt2.hash, ChunkRanges::all())
            .build();
        let mut progress = swarm
            .download(request, Shuffled::new(vec![node1_id, node2_id]))
            .stream()
            .await?;
        while progress.next().await.is_some() {}
        assert_eq!(store3.get_bytes(tt1.hash).await?.deref(), b"hello world");
        assert_eq!(store3.get_bytes(tt2.hash).await?.deref(), b"hello world 2");
        Ok(())
    }

    #[tokio::test]
    async fn downloader_get_smoke() -> TestResult<()> {
        // tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let (r2, store2, _, _) = node_test_setup_fs(testdir.path().join("b")).await?;
        let (r3, store3, _, sp3) = node_test_setup_fs(testdir.path().join("c")).await?;
        let tt1 = store1.add_slice(vec![1; 10000000]).await?;
        let tt2 = store2.add_slice(vec![2; 10000000]).await?;
        let hs = [tt1.hash, tt2.hash].into_iter().collect::<HashSeq>();
        let root = store1
            .add_bytes_with_opts(AddBytesOptions {
                data: hs.clone().into(),
                format: crate::BlobFormat::HashSeq,
            })
            .await?;
        let node1_addr = r1.endpoint().addr();
        let node1_id = node1_addr.id;
        let node2_addr = r2.endpoint().addr();
        let node2_id = node2_addr.id;
        let swarm = Downloader::new(&store3, r3.endpoint());
        sp3.add_endpoint_info(node1_addr.clone());
        sp3.add_endpoint_info(node2_addr.clone());
        let request = GetRequest::builder()
            .root(ChunkRanges::all())
            .next(ChunkRanges::all())
            .next(ChunkRanges::all())
            .build(root.hash);
        if true {
            let mut progress = swarm
                .download_with_opts(DownloadOptions::new(
                    request,
                    [node1_id, node2_id],
                    SplitStrategy::Split,
                ))
                .stream()
                .await?;
            while progress.next().await.is_some() {}
        }
        if false {
            let conn = r3.endpoint().connect(node1_addr, crate::ALPN).await?;
            let remote = store3.remote();
            let _rh = remote
                .execute_get(
                    conn.clone(),
                    GetRequest::builder()
                        .root(ChunkRanges::all())
                        .build(root.hash),
                )
                .await?;
            let h1 = remote.execute_get(
                conn.clone(),
                GetRequest::builder()
                    .child(0, ChunkRanges::all())
                    .build(root.hash),
            );
            let h2 = remote.execute_get(
                conn.clone(),
                GetRequest::builder()
                    .child(1, ChunkRanges::all())
                    .build(root.hash),
            );
            h1.await?;
            h2.await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn downloader_get_all() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let (r2, store2, _, _) = node_test_setup_fs(testdir.path().join("b")).await?;
        let (r3, store3, _, sp3) = node_test_setup_fs(testdir.path().join("c")).await?;
        let tt1 = store1.add_slice(vec![1; 10000000]).await?;
        let tt2 = store2.add_slice(vec![2; 10000000]).await?;
        let hs = [tt1.hash, tt2.hash].into_iter().collect::<HashSeq>();
        let root = store1
            .add_bytes_with_opts(AddBytesOptions {
                data: hs.clone().into(),
                format: crate::BlobFormat::HashSeq,
            })
            .await?;
        let node1_addr = r1.endpoint().addr();
        let node1_id = node1_addr.id;
        let node2_addr = r2.endpoint().addr();
        let node2_id = node2_addr.id;
        let swarm = Downloader::new(&store3, r3.endpoint());
        sp3.add_endpoint_info(node1_addr.clone());
        sp3.add_endpoint_info(node2_addr.clone());
        let request = GetRequest::all(root.hash);
        let mut progress = swarm
            .download_with_opts(DownloadOptions::new(
                request,
                [node1_id, node2_id],
                SplitStrategy::Split,
            ))
            .stream()
            .await?;
        while progress.next().await.is_some() {}
        Ok(())
    }

    /// Invariant: `DownloaderActor` must reap each `handle_download` task
    /// from its `JoinSet` as that task finishes, not only on shutdown.
    /// If steady-state reaping is skipped, every completed download
    /// retains its tokio task header for the lifetime of the actor and
    /// the heap grows linearly with download volume — invisible at the
    /// API surface, since downloads still report progress and finish.
    ///
    /// We submit many downloads with an empty provider list so each
    /// `handle_download` finishes in microseconds with no I/O, then
    /// assert the actor reaches an idle state via [`Downloader::wait_idle`].
    /// If the `tasks.join_next()` arm is removed from
    /// `DownloaderActor::run`'s `select!`, the JoinSet stays full of
    /// completed-but-not-joined tasks, `tasks.is_empty()` never becomes
    /// true, and the timeout below fires.
    ///
    /// The complementary `idle_waiters` notification path is not
    /// exercised here: by the time this test calls `wait_idle`, the
    /// actor has already drained the JoinSet during the per-stream
    /// awaits, so `wait_idle`'s fast path answers directly.
    #[tokio::test]
    async fn downloader_drains_completed_tasks() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r, store, _, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let swarm = Downloader::new(&store, r.endpoint());

        let n = 1_000;
        let bogus_hash = Hash::new(b"this hash is not stored anywhere");
        let mut streams = Vec::with_capacity(n);
        for _ in 0..n {
            streams.push(
                swarm
                    .download(GetRequest::all(bogus_hash), Shuffled::new(vec![]))
                    .stream()
                    .await?,
            );
        }
        for mut s in streams {
            while s.next().await.is_some() {}
        }

        tokio::time::timeout(std::time::Duration::from_secs(5), swarm.wait_idle())
            .await
            .map_err(|_| {
                "wait_idle did not resolve within 5s — DownloaderActor JoinSet not draining"
            })??;

        Ok(())
    }

    /// What a download reported, folded out of its progress stream.
    #[derive(Default, Debug)]
    struct Reported {
        tried: Vec<EndpointId>,
        failed: Vec<EndpointId>,
        completed: usize,
        bytes: u64,
        errors: usize,
    }

    async fn collect(
        mut stream: impl n0_future::Stream<Item = DownloadProgressItem> + Unpin,
    ) -> Reported {
        let mut out = Reported::default();
        while let Some(item) = stream.next().await {
            match item {
                DownloadProgressItem::TryProvider { id, .. } => out.tried.push(id),
                DownloadProgressItem::ProviderFailed { id, .. } => out.failed.push(id),
                DownloadProgressItem::PartComplete { .. } => out.completed += 1,
                DownloadProgressItem::BytesTransferred(n) => out.bytes += n,
                DownloadProgressItem::Error(_) | DownloadProgressItem::DownloadError => {
                    out.errors += 1
                }
                DownloadProgressItem::Progress(_) => {}
            }
        }
        out
    }

    /// Build a HashSeq of `children` in `store` and return (root hash, total
    /// payload bytes of root + children).
    async fn add_collection(
        store: &crate::store::fs::FsStore,
        children: &[Vec<u8>],
    ) -> TestResult<(Hash, u64)> {
        let mut hashes = Vec::new();
        let mut total = 0u64;
        for child in children {
            hashes.push(store.add_slice(child.clone()).await?.hash);
            total += child.len() as u64;
        }
        let hs = hashes.into_iter().collect::<HashSeq>();
        let root_bytes: bytes::Bytes = hs.into();
        total += root_bytes.len() as u64;
        let root = store
            .add_bytes_with_opts(AddBytesOptions {
                data: root_bytes,
                format: crate::BlobFormat::HashSeq,
            })
            .await?;
        Ok((root.hash, total))
    }

    /// Split downloads must report the provider that served the HashSeq root
    /// (it used to be swallowed by a `Drain` sink), and the summed
    /// `BytesTransferred` must equal the exact payload of root + children.
    #[tokio::test]
    async fn downloader_split_reports_root_and_exact_bytes() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let (r2, store2, _, sp2) = node_test_setup_fs(testdir.path().join("b")).await?;
        let children = vec![vec![1u8; 100_000], vec![2u8; 4096]];
        let (root, expected_bytes) = add_collection(&store1, &children).await?;
        let addr1 = r1.endpoint().addr();
        sp2.add_endpoint_info(addr1.clone());
        let swarm = Downloader::new(&store2, r2.endpoint());

        let reported = collect(
            swarm
                .download_with_opts(DownloadOptions::new(
                    GetRequest::all(root),
                    [addr1.id],
                    SplitStrategy::Split,
                ))
                .stream()
                .await?,
        )
        .await;

        assert_eq!(reported.errors, 0);
        // root + both children, each attributed to the one provider.
        assert_eq!(reported.tried, vec![addr1.id; 3]);
        assert!(reported.failed.is_empty());
        assert_eq!(reported.completed, 3);
        assert_eq!(reported.bytes, expected_bytes);
        Ok(())
    }

    /// A child that is already resident must neither be re-fetched nor reported
    /// as a provider attempt.
    #[tokio::test]
    async fn downloader_split_skips_resident_child() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let (r2, store2, _, sp2) = node_test_setup_fs(testdir.path().join("b")).await?;
        let resident = vec![1u8; 100_000];
        let missing = vec![2u8; 4096];
        let children = vec![resident.clone(), missing.clone()];
        let (root, all_bytes) = add_collection(&store1, &children).await?;
        // The consumer already holds the big child.
        store2.add_slice(resident.clone()).await?;
        let addr1 = r1.endpoint().addr();
        sp2.add_endpoint_info(addr1.clone());
        let swarm = Downloader::new(&store2, r2.endpoint());

        let reported = collect(
            swarm
                .download_with_opts(DownloadOptions::new(
                    GetRequest::all(root),
                    [addr1.id],
                    SplitStrategy::Split,
                ))
                .stream()
                .await?,
        )
        .await;

        assert_eq!(reported.errors, 0);
        // Root + the missing child only: the resident child never dials.
        assert_eq!(reported.tried, vec![addr1.id; 2]);
        assert_eq!(reported.completed, 2);
        assert_eq!(reported.bytes, all_bytes - resident.len() as u64);
        Ok(())
    }

    /// Every completed child must be reported, including the last one to
    /// finish. The fan-in used to be abandoned as soon as the futures stream
    /// ended, dropping whatever was still buffered; small children make that
    /// race likely, so this repeats with a fresh consumer each round.
    #[tokio::test]
    async fn downloader_split_reports_every_child_completion() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let children = (0..12u8).map(|i| vec![i; 1024]).collect::<Vec<_>>();
        let (root, expected_bytes) = add_collection(&store1, &children).await?;
        let addr1 = r1.endpoint().addr();

        for round in 0..3 {
            let (r2, store2, _, sp2) =
                node_test_setup_fs(testdir.path().join(format!("consumer-{round}"))).await?;
            sp2.add_endpoint_info(addr1.clone());
            let swarm = Downloader::new(&store2, r2.endpoint());
            let reported = collect(
                swarm
                    .download_with_opts(DownloadOptions::new(
                        GetRequest::all(root),
                        [addr1.id],
                        SplitStrategy::Split,
                    ))
                    .stream()
                    .await?,
            )
            .await;
            assert_eq!(reported.errors, 0, "round {round}");
            assert_eq!(reported.completed, children.len() + 1, "round {round}");
            assert_eq!(reported.tried.len(), children.len() + 1, "round {round}");
            assert_eq!(reported.bytes, expected_bytes, "round {round}");
            r2.shutdown().await?;
        }
        Ok(())
    }

    /// Failover across an ordered provider list is reported per request: the
    /// provider that cannot serve is marked failed, the next one completes, and
    /// the byte total still covers the content exactly once.
    #[tokio::test]
    async fn downloader_split_reports_failover() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("holder")).await?;
        let (r2, _store2, _, _) = node_test_setup_fs(testdir.path().join("empty")).await?;
        let (r3, store3, _, sp3) = node_test_setup_fs(testdir.path().join("consumer")).await?;
        let children = vec![vec![7u8; 8192], vec![9u8; 8192]];
        let (root, expected_bytes) = add_collection(&store1, &children).await?;
        let holder = r1.endpoint().addr();
        let empty = r2.endpoint().addr();
        sp3.add_endpoint_info(holder.clone());
        sp3.add_endpoint_info(empty.clone());
        let swarm = Downloader::new(&store3, r3.endpoint());

        let reported = collect(
            swarm
                .download_with_opts(DownloadOptions::new(
                    GetRequest::all(root),
                    [empty.id, holder.id],
                    SplitStrategy::Split,
                ))
                .stream()
                .await?,
        )
        .await;

        assert_eq!(reported.errors, 0);
        // Root + 2 children, each walking the list from the top.
        assert_eq!(reported.failed, vec![empty.id; 3]);
        assert_eq!(reported.completed, 3);
        assert_eq!(reported.bytes, expected_bytes);
        Ok(())
    }
}
