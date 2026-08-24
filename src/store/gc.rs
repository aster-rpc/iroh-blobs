use std::{collections::HashSet, pin::Pin, sync::Arc};

use bao_tree::ChunkRanges;
use genawaiter::sync::{Co, Gen};
use n0_future::{time::Duration, Stream, StreamExt};
use tracing::{debug, error, info, warn};

use crate::{api::Store, Hash, HashAndFormat};

/// An event related to GC
#[derive(Debug)]
pub enum GcMarkEvent {
    /// A custom event (info)
    CustomDebug(String),
    /// A custom non critical error
    CustomWarning(String, Option<crate::api::Error>),
    /// A non-raw root could not be fully enumerated, so the live set does not
    /// bound its collection. A sweep over such a live set would delete
    /// children the mark could not see — the members of a claimed collection
    /// whose root has not arrived yet — so the cycle's sweep must be skipped.
    TraversalIncomplete(Hash),
    /// An unrecoverable error during GC
    Error(crate::api::Error),
}

/// An event related to GC
#[derive(Debug)]
pub enum GcSweepEvent {
    /// A custom event (debug)
    CustomDebug(String),
    /// A custom non critical error
    #[allow(dead_code)]
    CustomWarning(String, Option<crate::api::Error>),
    /// An unrecoverable error during GC
    Error(crate::api::Error),
}

/// Compute the set of live hashes
pub(super) async fn gc_mark_task(
    store: &Store,
    live: &mut HashSet<Hash>,
    co: &Co<GcMarkEvent>,
) -> crate::api::Result<()> {
    macro_rules! trace {
        ($($arg:tt)*) => {
            co.yield_(GcMarkEvent::CustomDebug(format!($($arg)*))).await;
        };
    }
    macro_rules! warn {
        ($($arg:tt)*) => {
            co.yield_(GcMarkEvent::CustomWarning(format!($($arg)*), None)).await;
        };
    }
    let mut roots = HashSet::new();
    trace!("traversing tags");
    let mut tags = store.tags().list().await?;
    while let Some(tag) = tags.next().await {
        let info = tag?;
        trace!("adding root {:?} {:?}", info.name, info.hash_and_format());
        roots.insert(info.hash_and_format());
    }
    trace!("traversing temp roots");
    let mut tts = store.tags().list_temp_tags().await?;
    while let Some(tt) = tts.next().await {
        trace!("adding temp root {:?}", tt);
        roots.insert(tt);
    }
    for HashAndFormat { hash, format } in roots {
        // we need to do this for all formats except raw
        // Insert and traverse are SEPARATE decisions. `HashAndFormat`'s
        // identity includes the format, so the roots can hold both (H, Raw)
        // and (H, HashSeq) — and H may already be live through external
        // protection or another collection's membership. Joining traversal to
        // `live.insert(hash)` made a collection walk conditional on the ROOT
        // HASH being novel: an aliased or pre-live HashSeq root was never
        // traversed, its children went unmarked (swept if complete), and an
        // incomplete root emitted no `TraversalIncomplete`, silently
        // bypassing the sweep suppression.
        let _ = live.insert(hash);
        if !format.is_raw() {
            let mut stream = store.export_bao(hash, ChunkRanges::all()).hashes();
            while let Some(child) = stream.next().await {
                match child {
                    Ok(child) => {
                        live.insert(child);
                    }
                    Err(e) => {
                        warn!("error while traversing hashseq: {e:?}");
                        // The collection is unbounded from here: some of its
                        // members may be resident and were not marked live.
                        co.yield_(GcMarkEvent::TraversalIncomplete(hash)).await;
                    }
                }
            }
        }
    }
    trace!("gc mark done. found {} live blobs", live.len());
    Ok(())
}

async fn gc_sweep_task(
    store: &Store,
    live: &HashSet<Hash>,
    co: &Co<GcSweepEvent>,
) -> crate::api::Result<()> {
    let mut blobs = store.blobs().list().stream().await?;
    let mut count = 0;
    let mut batch = Vec::new();
    while let Some(hash) = blobs.next().await {
        let hash = hash?;
        if !live.contains(&hash) {
            batch.push(hash);
            count += 1;
        }
        if batch.len() >= 100 {
            store.blobs().delete(batch.clone()).await?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        store.blobs().delete(batch).await?;
    }
    store.sync_db().await?;
    co.yield_(GcSweepEvent::CustomDebug(format!("deleted {count} blobs")))
        .await;
    Ok(())
}

fn gc_mark<'a>(
    store: &'a Store,
    live: &'a mut HashSet<Hash>,
) -> impl Stream<Item = GcMarkEvent> + 'a {
    Gen::new(|co| async move {
        if let Err(e) = gc_mark_task(store, live, &co).await {
            co.yield_(GcMarkEvent::Error(e)).await;
        }
    })
}

fn gc_sweep<'a>(
    store: &'a Store,
    live: &'a HashSet<Hash>,
) -> impl Stream<Item = GcSweepEvent> + 'a {
    Gen::new(|co| async move {
        if let Err(e) = gc_sweep_task(store, live, &co).await {
            co.yield_(GcSweepEvent::Error(e)).await;
        }
    })
}

/// Configuration for garbage collection.
///
/// To protect blobs during long-running writes without pausing the GC
/// schedule, use [`crate::api::blobs::Batch::temp_tag`].
#[derive(derive_more::Debug, Clone)]
pub struct GcConfig {
    /// Interval in which to run garbage collection.
    pub interval: Duration,
    /// Optional callback to manually add protected blobs.
    ///
    /// The callback is called before each garbage collection run. It gets a `&mut HashSet<Hash>`
    /// and returns a future that returns [`ProtectOutcome`]. All hashes that are added to the
    /// [`HashSet`] will be protected from garbage collection during this run.
    ///
    /// In normal operation, return [`ProtectOutcome::Continue`] from the callback. If you return
    /// [`ProtectOutcome::Abort`], the garbage collection run will be aborted.Use this if your
    /// source of hashes to protect returned an error, and thus garbage collection should be skipped
    /// completely to not unintentionally delete blobs that should be protected.
    #[debug("ProtectCallback")]
    pub add_protected: Option<ProtectCb>,
}

/// Returned from [`ProtectCb`].
///
/// See [`GcConfig::add_protected] for details.
#[derive(Debug)]
pub enum ProtectOutcome {
    /// Continue with the garbage collection run.
    Continue,
    /// Abort the garbage collection run.
    Abort,
}

/// The type of the garbage collection callback.
///
/// See [`GcConfig::add_protected] for details.
pub type ProtectCb = Arc<
    dyn for<'a> Fn(
            &'a mut HashSet<Hash>,
        )
            -> Pin<Box<dyn std::future::Future<Output = ProtectOutcome> + Send + Sync + 'a>>
        + Send
        + Sync
        + 'static,
>;

pub async fn gc_run_once(store: &Store, live: &mut HashSet<Hash>) -> crate::api::Result<()> {
    debug!(externally_protected = live.len(), "gc: start");
    let mut sweep_safe = true;
    {
        store.clear_protected().await?;
        let mut stream = gc_mark(store, live);
        while let Some(ev) = stream.next().await {
            match ev {
                GcMarkEvent::CustomDebug(msg) => {
                    debug!("{}", msg);
                }
                GcMarkEvent::CustomWarning(msg, err) => {
                    warn!("{}: {:?}", msg, err);
                }
                GcMarkEvent::TraversalIncomplete(root) => {
                    // A claimed collection the mark cannot bound: sweeping now
                    // would delete resident children the live set missed. Skip
                    // this cycle's sweep; once the root arrives, a later mark
                    // enumerates the collection and sweeping resumes. Loud,
                    // because a root that never arrives makes the store
                    // grow-only until the claim is released.
                    warn!(
                        "gc: skipping sweep — collection root {} is claimed but not \
                         enumerable yet",
                        root.to_hex()
                    );
                    sweep_safe = false;
                }
                GcMarkEvent::Error(err) => {
                    error!("error during gc mark: {:?}", err);
                    return Err(err);
                }
            }
        }
    }
    if !sweep_safe {
        debug!("gc: mark incomplete, sweep skipped");
        return Ok(());
    }
    debug!(total_protected = live.len(), "gc: sweep");
    {
        let mut stream = gc_sweep(store, live);
        while let Some(ev) = stream.next().await {
            match ev {
                GcSweepEvent::CustomDebug(msg) => {
                    debug!("{}", msg);
                }
                GcSweepEvent::CustomWarning(msg, err) => {
                    warn!("{}: {:?}", msg, err);
                }
                GcSweepEvent::Error(err) => {
                    error!("error during gc sweep: {:?}", err);
                    return Err(err);
                }
            }
        }
    }
    debug!("gc: done");

    Ok(())
}

pub async fn run_gc(store: Store, config: GcConfig) {
    debug!("gc enabled with interval {:?}", config.interval);
    let mut live = HashSet::new();
    loop {
        live.clear();
        n0_future::time::sleep(config.interval).await;
        if let Some(ref cb) = config.add_protected {
            match (cb)(&mut live).await {
                ProtectOutcome::Continue => {}
                ProtectOutcome::Abort => {
                    info!("abort gc run: protect callback indicated abort");
                    continue;
                }
            }
        }
        if let Err(e) = gc_run_once(&store, &mut live).await {
            error!("error during gc run: {e}");
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self};

    use bao_tree::io::EncodeError;
    use range_collections::RangeSet2;
    use testresult::TestResult;

    use super::*;
    use crate::{
        api::{blobs::AddBytesOptions, ExportBaoError, RequestError, Store},
        hashseq::HashSeq,
        BlobFormat,
    };

    async fn gc_smoke(store: &Store) -> TestResult<()> {
        let blobs = store.blobs();
        let at = blobs.add_slice("a").temp_tag().await?;
        let bt = blobs.add_slice("b").temp_tag().await?;
        let ct = blobs.add_slice("c").temp_tag().await?;
        let dt = blobs.add_slice("d").temp_tag().await?;
        let et = blobs.add_slice("e").temp_tag().await?;
        let ft = blobs.add_slice("f").temp_tag().await?;
        let gt = blobs.add_slice("g").temp_tag().await?;
        let ht = blobs.add_slice("h").with_named_tag("h").await?;
        let a = at.hash();
        let b = bt.hash();
        let c = ct.hash();
        let d = dt.hash();
        let e = et.hash();
        let f = ft.hash();
        let g = gt.hash();
        let h = ht.hash;
        store.tags().set("c", ct.hash_and_format()).await?;
        let dehs = [d, e].into_iter().collect::<HashSeq>();
        let hehs = blobs
            .add_bytes_with_opts(AddBytesOptions {
                data: dehs.into(),
                format: BlobFormat::HashSeq,
            })
            .await?;
        let fghs = [f, g].into_iter().collect::<HashSeq>();
        let fghs = blobs
            .add_bytes_with_opts(AddBytesOptions {
                data: fghs.into(),
                format: BlobFormat::HashSeq,
            })
            .temp_tag()
            .await?;
        store.tags().set("fg", fghs.hash_and_format()).await?;
        drop(fghs);
        drop(bt);
        store.tags().delete("h").await?;
        let mut live = HashSet::new();
        gc_run_once(store, &mut live).await?;
        // a is protected because we keep the temp tag
        assert!(live.contains(&a));
        assert!(store.has(a).await?);
        // b is not protected because we drop the temp tag
        assert!(!live.contains(&b));
        assert!(!store.has(b).await?);
        // c is protected because we set an explicit tag
        assert!(live.contains(&c));
        assert!(store.has(c).await?);
        // d and e are protected because they are part of a hashseq protected by a temp tag
        assert!(live.contains(&d));
        assert!(store.has(d).await?);
        assert!(live.contains(&e));
        assert!(store.has(e).await?);
        // f and g are protected because they are part of a hashseq protected by a tag
        assert!(live.contains(&f));
        assert!(store.has(f).await?);
        assert!(live.contains(&g));
        assert!(store.has(g).await?);
        // h is not protected because we deleted the tag before gc ran
        assert!(!live.contains(&h));
        assert!(!store.has(h).await?);
        drop(at);
        drop(hehs);
        Ok(())
    }

    #[cfg(feature = "fs-store")]
    async fn gc_file_delete(path: &std::path::Path, store: &Store) -> TestResult<()> {
        use bao_tree::ChunkNum;

        use crate::store::{fs::options::PathOptions, util::tests::create_n0_bao};
        let mut live = HashSet::new();
        let options = PathOptions::new(&path.join("db"));
        // create a large complete file and check that the data and outboard files are deleted by gc
        {
            let a = store
                .blobs()
                .add_slice(vec![0u8; 8000000])
                .temp_tag()
                .await?;
            let ah = a.hash();
            let data_path = options.data_path(&ah);
            let outboard_path = options.outboard_path(&ah);
            assert!(data_path.exists());
            assert!(outboard_path.exists());
            assert!(store.has(ah).await?);
            drop(a);
            gc_run_once(store, &mut live).await?;
            assert!(!data_path.exists());
            assert!(!outboard_path.exists());
        }
        live.clear();
        // create a large partial file and check that the data and outboard file as well as
        // the sizes and bitfield files are deleted by gc
        {
            let data = vec![1u8; 8000000];
            let ranges = ChunkRanges::from(..ChunkNum(19));
            let (bh, b_bao) = create_n0_bao(&data, &ranges)?;
            store.import_bao_bytes(bh, ranges, b_bao).await?;
            let data_path = options.data_path(&bh);
            let outboard_path = options.outboard_path(&bh);
            let sizes_path = options.sizes_path(&bh);
            let bitfield_path = options.bitfield_path(&bh);
            store.wait_idle().await?;
            assert!(data_path.exists());
            assert!(outboard_path.exists());
            assert!(sizes_path.exists());
            assert!(bitfield_path.exists());
            gc_run_once(store, &mut live).await?;
            assert!(!data_path.exists());
            assert!(!outboard_path.exists());
            assert!(!sizes_path.exists());
            assert!(!bitfield_path.exists());
        }
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "fs-store")]
    async fn gc_smoke_fs() -> TestResult {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let db_path = testdir.path().join("db");
        let store = crate::store::fs::FsStore::load(&db_path).await?;
        gc_smoke(&store).await?;
        gc_file_delete(testdir.path(), &store).await?;
        Ok(())
    }

    #[tokio::test]
    async fn gc_smoke_mem() -> TestResult {
        tracing_subscriber::fmt::try_init().ok();
        let store = crate::store::mem::MemStore::new();
        gc_smoke(&store).await?;
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "fs-store")]
    async fn gc_check_deletion_fs() -> TestResult {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let db_path = testdir.path().join("db");
        let store = crate::store::fs::FsStore::load(&db_path).await?;
        gc_check_deletion(&store).await
    }

    #[tokio::test]
    async fn gc_check_deletion_mem() -> TestResult {
        tracing_subscriber::fmt::try_init().ok();
        let store = crate::store::mem::MemStore::default();
        gc_check_deletion(&store).await
    }

    /// A tag created between a sweep's mark and its delete must protect the
    /// blob it names.
    ///
    /// `gc_mark` snapshots the tags table and the temp roots; the delete phase
    /// re-checks only the store's protected set. A blob that was resident and
    /// unreferenced when mark ran, then claimed while the sweep was still in
    /// flight — a batch temp tag, a persistent tag, or both, which is exactly
    /// the already-local promotion order a caller is documented to use — was
    /// deleted by that same sweep, leaving a dangling tag over lost content.
    /// Tag creation must join the in-flight sweep's protection instead.
    async fn tag_after_mark_survives_the_sweep(store: &Store) -> TestResult<()> {
        let blobs = store.blobs();

        // Two resident, unreferenced blobs: one will be claimed under a temp
        // guard then promoted, one by a bare persistent tag.
        let guarded_data = b"claimed under a live batch guard".to_vec();
        let tagged_data = b"claimed by a bare persistent tag".to_vec();
        let t1 = blobs.add_slice(&guarded_data).temp_tag().await?;
        let guarded = t1.hash();
        drop(t1);
        let t2 = blobs.add_slice(&tagged_data).temp_tag().await?;
        let tagged = t2.hash();
        drop(t2);

        // The sweep's mark runs first: no roots, so neither hash is live.
        store.blobs().clear_protected().await?;
        let mut live = HashSet::new();
        {
            let mut mark = gc_mark(store, &mut live);
            while let Some(ev) = mark.next().await {
                if let GcMarkEvent::Error(e) = ev {
                    return Err(e.into());
                }
            }
        }
        assert!(!live.contains(&guarded) && !live.contains(&tagged));

        // The claims land AFTER the mark, before the delete.
        let batch = blobs.batch().await?;
        let guard = batch.temp_tag(HashAndFormat::raw(guarded)).await?;
        store
            .tags()
            .set("claimed-under-guard", HashAndFormat::raw(guarded))
            .await?;
        store
            .tags()
            .set("claimed-bare", HashAndFormat::raw(tagged))
            .await?;

        // The same sweep's delete phase runs with its stale live set.
        {
            let mut sweep = gc_sweep(store, &live);
            while let Some(ev) = sweep.next().await {
                if let GcSweepEvent::Error(e) = ev {
                    return Err(e.into());
                }
            }
        }
        drop(guard);
        drop(batch);

        // Both tagged blobs survived that sweep and are readable.
        assert_eq!(
            store.get_bytes(guarded).await?.as_ref(),
            guarded_data.as_slice(),
            "a blob claimed under a live batch guard after mark was deleted by that sweep",
        );
        assert_eq!(
            store.get_bytes(tagged).await?.as_ref(),
            tagged_data.as_slice(),
            "a blob claimed by a persistent tag after mark was deleted by that sweep",
        );

        // And an ordinary next cycle still respects the tags (they are in the
        // fresh mark's snapshot) …
        let mut live2 = HashSet::new();
        gc_run_once(store, &mut live2).await?;
        assert_eq!(store.get_bytes(guarded).await?.as_ref(), guarded_data.as_slice());
        assert_eq!(store.get_bytes(tagged).await?.as_ref(), tagged_data.as_slice());

        // … and deleting them makes both reclaimable: protection joined the
        // sweep, it did not leak past the tags' lifetimes.
        store.tags().delete("claimed-under-guard").await?;
        store.tags().delete("claimed-bare").await?;
        let mut live3 = HashSet::new();
        gc_run_once(store, &mut live3).await?;
        assert!(store.get_bytes(guarded).await.is_err());
        assert!(store.get_bytes(tagged).await.is_err());
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "fs-store")]
    async fn tag_after_mark_survives_the_sweep_fs() -> TestResult {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let store = crate::store::fs::FsStore::load(testdir.path().join("db")).await?;
        tag_after_mark_survives_the_sweep(&store).await
    }

    #[tokio::test]
    async fn tag_after_mark_survives_the_sweep_mem() -> TestResult {
        tracing_subscriber::fmt::try_init().ok();
        let store = crate::store::mem::MemStore::new();
        tag_after_mark_survives_the_sweep(&store).await
    }

    /// The HashSeq form of the same claim: a collection tag created between a
    /// sweep's mark and its delete must protect the ROOT AND EVERY CHILD.
    ///
    /// The root is pinned by the tag itself; the children are only reachable
    /// through the root's bytes, which the stale mark never traversed (the tag
    /// did not exist yet). Custody expansion at tag creation is what joins
    /// them to the in-flight sweep's protection — and a temp-only HashSeq tag
    /// must protect at least its root (`TempTags::contains` was raw-only).
    async fn hash_seq_tag_after_mark_survives_the_sweep(store: &Store) -> TestResult<()> {
        use crate::{api::blobs::AddBytesOptions, hashseq::HashSeq, BlobFormat};
        let blobs = store.blobs();

        // Two collections, each with two resident children: one claimed by a
        // batch temp tag, one by a bare persistent tag. Everything resident
        // and unreferenced before mark.
        let mk_child = |data: &'static [u8]| async move {
            let tt = blobs.add_slice(data).temp_tag().await?;
            let hash = tt.hash();
            drop(tt);
            TestResult::<Hash>::Ok(hash)
        };
        let a = mk_child(b"child a").await?;
        let b = mk_child(b"child b").await?;
        let c = mk_child(b"child c").await?;
        let d = mk_child(b"child d").await?;
        let mk_seq = |children: [Hash; 2]| async move {
            let seq: HashSeq = children.into_iter().collect();
            let tt = blobs
                .add_bytes_with_opts(AddBytesOptions {
                    data: seq.into(),
                    format: BlobFormat::HashSeq,
                })
                .temp_tag()
                .await?;
            let hash = tt.hash();
            drop(tt);
            TestResult::<Hash>::Ok(hash)
        };
        let guarded_root = mk_seq([a, b]).await?;
        let tagged_root = mk_seq([c, d]).await?;

        // The sweep's mark runs before any claim exists.
        store.blobs().clear_protected().await?;
        let mut live = HashSet::new();
        {
            let mut mark = gc_mark(store, &mut live);
            while let Some(ev) = mark.next().await {
                if let GcMarkEvent::Error(e) = ev {
                    return Err(e.into());
                }
            }
        }
        for h in [guarded_root, tagged_root, a, b, c, d] {
            assert!(!live.contains(&h));
        }

        // The claims land after mark: a temp guard on one collection, a bare
        // persistent tag on the other.
        let batch = blobs.batch().await?;
        let guard = batch
            .temp_tag(HashAndFormat::hash_seq(guarded_root))
            .await?;
        store
            .tags()
            .set("claimed-seq", HashAndFormat::hash_seq(tagged_root))
            .await?;

        // The same sweep's delete phase runs with its stale live set.
        {
            let mut sweep = gc_sweep(store, &live);
            while let Some(ev) = sweep.next().await {
                if let GcSweepEvent::Error(e) = ev {
                    return Err(e.into());
                }
            }
        }
        drop(guard);
        drop(batch);

        // Roots AND children survived.
        assert_eq!(store.get_bytes(a).await?.as_ref(), b"child a");
        assert_eq!(store.get_bytes(b).await?.as_ref(), b"child b");
        assert_eq!(store.get_bytes(c).await?.as_ref(), b"child c");
        assert_eq!(store.get_bytes(d).await?.as_ref(), b"child d");
        assert!(store.get_bytes(guarded_root).await.is_ok());
        assert!(store.get_bytes(tagged_root).await.is_ok());

        // An ordinary next cycle still respects the persistent tag's whole
        // collection (fresh mark traverses it), while the released temp
        // guard's collection is reclaimed.
        let mut live2 = HashSet::new();
        gc_run_once(store, &mut live2).await?;
        assert!(store.get_bytes(c).await.is_ok());
        assert!(store.get_bytes(d).await.is_ok());
        assert!(store.get_bytes(guarded_root).await.is_err());
        assert!(store.get_bytes(a).await.is_err());
        assert!(store.get_bytes(b).await.is_err());

        // Releasing the persistent tag reclaims its collection too: the
        // expansion never outlives the tag past the next cycle.
        store.tags().delete("claimed-seq").await?;
        let mut live3 = HashSet::new();
        gc_run_once(store, &mut live3).await?;
        assert!(store.get_bytes(tagged_root).await.is_err());
        assert!(store.get_bytes(c).await.is_err());
        assert!(store.get_bytes(d).await.is_err());
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "fs-store")]
    async fn hash_seq_tag_after_mark_survives_the_sweep_fs() -> TestResult {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let store = crate::store::fs::FsStore::load(testdir.path().join("db")).await?;
        hash_seq_tag_after_mark_survives_the_sweep(&store).await
    }

    #[tokio::test]
    async fn hash_seq_tag_after_mark_survives_the_sweep_mem() -> TestResult {
        tracing_subscriber::fmt::try_init().ok();
        let store = crate::store::mem::MemStore::new();
        hash_seq_tag_after_mark_survives_the_sweep(&store).await
    }

    /// A HashSeq claim whose root has NOT arrived yet still protects the
    /// collection — including children that are already resident.
    ///
    /// At claim time nobody can enumerate the children (the root's bytes do
    /// not exist locally), and when the root later arrives, resident children
    /// receive no write event (split downloads skip complete children). So
    /// expansion alone cannot cover this transition; the claim must instead
    /// stop the in-flight sweep (poison), and a mark that cannot enumerate a
    /// claimed root must skip its own sweep. Staged both ways: the same stale
    /// sweep racing a root that arrives mid-sweep, and a full GC cycle run
    /// while a claimed root is still absent.
    async fn late_root_hash_seq_claim_survives_the_sweep(store: &Store) -> TestResult<()> {
        use crate::{api::blobs::AddBytesOptions, hashseq::HashSeq, BlobFormat};
        let blobs = store.blobs();

        let mk_child = |data: &'static [u8]| async move {
            let tt = blobs.add_slice(data).temp_tag().await?;
            let hash = tt.hash();
            drop(tt);
            TestResult::<Hash>::Ok(hash)
        };
        // Children resident and unreferenced; the roots' BYTES are computed
        // but deliberately not imported yet.
        let a = mk_child(b"late child a").await?;
        let b = mk_child(b"late child b").await?;
        let c = mk_child(b"late child c").await?;
        let d = mk_child(b"late child d").await?;
        let seq_ab: HashSeq = [a, b].into_iter().collect();
        let seq_cd: HashSeq = [c, d].into_iter().collect();
        let bytes_ab: bytes::Bytes = seq_ab.into();
        let bytes_cd: bytes::Bytes = seq_cd.into();
        let root_ab = Hash::new(&bytes_ab);
        let root_cd = Hash::new(&bytes_cd);

        // Mark runs first: nothing is claimed, nothing is live.
        store.blobs().clear_protected().await?;
        let mut live = HashSet::new();
        {
            let mut mark = gc_mark(store, &mut live);
            while let Some(ev) = mark.next().await {
                if let GcMarkEvent::Error(e) = ev {
                    return Err(e.into());
                }
            }
        }

        // The claims land with ABSENT roots: one temp guard, one persistent
        // tag. Acknowledging them poisons the in-flight sweep — their members
        // are unknowable.
        let batch = blobs.batch().await?;
        let guard = batch.temp_tag(HashAndFormat::hash_seq(root_ab)).await?;
        store
            .tags()
            .set("late-claim", HashAndFormat::hash_seq(root_cd))
            .await?;

        // The roots arrive mid-sweep-window. Their pre-existing children get
        // no write event for this.
        for data in [bytes_ab.clone(), bytes_cd.clone()] {
            let tt = blobs
                .add_bytes_with_opts(AddBytesOptions {
                    data,
                    format: BlobFormat::HashSeq,
                })
                .temp_tag()
                .await?;
            drop(tt);
        }

        // The stale sweep runs — and must delete nothing.
        {
            let mut sweep = gc_sweep(store, &live);
            while let Some(ev) = sweep.next().await {
                if let GcSweepEvent::Error(e) = ev {
                    return Err(e.into());
                }
            }
        }
        for (h, data) in [
            (a, b"late child a".as_slice()),
            (b, b"late child b".as_slice()),
            (c, b"late child c".as_slice()),
            (d, b"late child d".as_slice()),
        ] {
            assert_eq!(
                store.get_bytes(h).await?.as_ref(),
                data,
                "a resident child of a late-root claim was deleted by the stale sweep",
            );
        }
        assert!(store.get_bytes(root_ab).await.is_ok());
        assert!(store.get_bytes(root_cd).await.is_ok());

        // An ordinary next cycle: both roots are complete now, the mark
        // traverses them, everything claimed stays.
        let mut live2 = HashSet::new();
        gc_run_once(store, &mut live2).await?;
        for h in [a, b, c, d, root_ab, root_cd] {
            assert!(store.get_bytes(h).await.is_ok());
        }

        // The full-cycle form: a third claim whose root NEVER arrives makes
        // the mark unable to bound the collection, so the whole sweep is
        // skipped — proven by releasing the temp guard: its collection would
        // be reclaimable, and must survive the skipped sweep anyway.
        let e = mk_child(b"late child e").await?;
        let f = mk_child(b"late child f").await?;
        let seq_ef: HashSeq = [e, f].into_iter().collect();
        let bytes_ef: bytes::Bytes = seq_ef.into();
        let root_ef = Hash::new(&bytes_ef);
        store
            .tags()
            .set("unresolvable-claim", HashAndFormat::hash_seq(root_ef))
            .await?;
        drop(guard);
        drop(batch);
        let mut live3 = HashSet::new();
        gc_run_once(store, &mut live3).await?;
        for h in [a, b, root_ab, e, f] {
            assert!(
                store.get_bytes(h).await.is_ok(),
                "a cycle whose mark cannot enumerate a claimed root must not sweep",
            );
        }

        // Releasing the unresolvable claim lets GC resume: the released
        // guard's collection is reclaimed, the persistent tag's stays.
        store.tags().delete("unresolvable-claim").await?;
        let mut live4 = HashSet::new();
        gc_run_once(store, &mut live4).await?;
        assert!(store.get_bytes(root_ab).await.is_err());
        assert!(store.get_bytes(a).await.is_err());
        assert!(store.get_bytes(b).await.is_err());
        assert!(store.get_bytes(e).await.is_err());
        assert!(store.get_bytes(f).await.is_err());
        assert!(store.get_bytes(c).await.is_ok());
        assert!(store.get_bytes(d).await.is_ok());

        // And releasing the last claim reclaims the rest.
        store.tags().delete("late-claim").await?;
        let mut live5 = HashSet::new();
        gc_run_once(store, &mut live5).await?;
        assert!(store.get_bytes(root_cd).await.is_err());
        assert!(store.get_bytes(c).await.is_err());
        assert!(store.get_bytes(d).await.is_err());
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "fs-store")]
    async fn late_root_hash_seq_claim_survives_the_sweep_fs() -> TestResult {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let store = crate::store::fs::FsStore::load(testdir.path().join("db")).await?;
        late_root_hash_seq_claim_survives_the_sweep(&store).await
    }

    #[tokio::test]
    async fn late_root_hash_seq_claim_survives_the_sweep_mem() -> TestResult {
        tracing_subscriber::fmt::try_init().ok();
        let store = crate::store::mem::MemStore::new();
        late_root_hash_seq_claim_survives_the_sweep(&store).await
    }

    /// Mark must traverse a HashSeq root even when its HASH is already live.
    ///
    /// `HashAndFormat` identity includes the format, so (H, Raw) and
    /// (H, HashSeq) are distinct roots over one hash — and H can be live
    /// before the collection root is visited (external protection, an aliased
    /// raw claim, or membership in another collection). Traversal joined to
    /// `live.insert(H)` skipped the walk in all of those: complete children
    /// went unmarked and were swept; an incomplete root emitted no
    /// `TraversalIncomplete`, bypassing the sweep suppression. The
    /// pre-seeded-`live` stagings here fail deterministically under the
    /// joined condition, independent of root-set iteration order.
    async fn already_live_hash_seq_root_is_still_traversed(store: &Store) -> TestResult<()> {
        use crate::{api::blobs::AddBytesOptions, hashseq::HashSeq, BlobFormat};
        let blobs = store.blobs();

        let mk_child = |data: &'static [u8]| async move {
            let tt = blobs.add_slice(data).temp_tag().await?;
            let hash = tt.hash();
            drop(tt);
            TestResult::<Hash>::Ok(hash)
        };

        // (A) A COMPLETE collection whose root hash is externally protected
        // (pre-seeded into `live`): its children must still be marked.
        let a = mk_child(b"aliased child a").await?;
        let b = mk_child(b"aliased child b").await?;
        let seq_ab: HashSeq = [a, b].into_iter().collect();
        let root_ab = {
            let tt = blobs
                .add_bytes_with_opts(AddBytesOptions {
                    data: seq_ab.into(),
                    format: BlobFormat::HashSeq,
                })
                .temp_tag()
                .await?;
            let hash = tt.hash();
            drop(tt);
            hash
        };
        store
            .tags()
            .set("aliased-seq", HashAndFormat::hash_seq(root_ab))
            .await?;

        let mut live = HashSet::new();
        live.insert(root_ab); // externally protected BEFORE the mark visits the root
        gc_run_once(store, &mut live).await?;
        assert_eq!(
            store.get_bytes(a).await?.as_ref(),
            b"aliased child a",
            "an already-live HashSeq root was not traversed and its child was swept",
        );
        assert_eq!(store.get_bytes(b).await?.as_ref(), b"aliased child b");
        assert!(store.get_bytes(root_ab).await.is_ok());

        // (B) The same hash claimed as BOTH Raw and HashSeq keeps its
        // children regardless of which form the root-set iteration meets
        // first.
        store
            .tags()
            .set("aliased-raw", HashAndFormat::raw(root_ab))
            .await?;
        let mut live2 = HashSet::new();
        gc_run_once(store, &mut live2).await?;
        assert!(store.get_bytes(a).await.is_ok());
        assert!(store.get_bytes(b).await.is_ok());

        // Dropping the HashSeq claim while the raw claim stays: the children
        // are no longer held (the raw form pins only the root's bytes).
        store.tags().delete("aliased-seq").await?;
        let mut live3 = HashSet::new();
        gc_run_once(store, &mut live3).await?;
        assert!(store.get_bytes(root_ab).await.is_ok());
        assert!(store.get_bytes(a).await.is_err());
        assert!(store.get_bytes(b).await.is_err());
        store.tags().delete("aliased-raw").await?;

        // (C) An already-live but ABSENT HashSeq root must still suppress the
        // sweep: the mark cannot bound the claimed collection even though the
        // root hash was not novel.
        let c = mk_child(b"aliased child c").await?;
        let d = mk_child(b"aliased child d").await?;
        let seq_cd: HashSeq = [c, d].into_iter().collect();
        let bytes_cd: bytes::Bytes = seq_cd.into();
        let root_cd = Hash::new(&bytes_cd); // never imported
        store
            .tags()
            .set("aliased-absent-seq", HashAndFormat::hash_seq(root_cd))
            .await?;
        let mut live4 = HashSet::new();
        live4.insert(root_cd); // externally protected: the root hash is not novel
        gc_run_once(store, &mut live4).await?;
        assert!(
            store.get_bytes(c).await.is_ok(),
            "an already-live absent HashSeq root did not suppress the sweep",
        );
        assert!(store.get_bytes(d).await.is_ok());

        // Releasing the unresolvable claim lets GC reclaim everything.
        store.tags().delete("aliased-absent-seq").await?;
        let mut live5 = HashSet::new();
        gc_run_once(store, &mut live5).await?;
        assert!(store.get_bytes(root_ab).await.is_err());
        assert!(store.get_bytes(c).await.is_err());
        assert!(store.get_bytes(d).await.is_err());
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "fs-store")]
    async fn already_live_hash_seq_root_is_still_traversed_fs() -> TestResult {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let store = crate::store::fs::FsStore::load(testdir.path().join("db")).await?;
        already_live_hash_seq_root_is_still_traversed(&store).await
    }

    #[tokio::test]
    async fn already_live_hash_seq_root_is_still_traversed_mem() -> TestResult {
        tracing_subscriber::fmt::try_init().ok();
        let store = crate::store::mem::MemStore::new();
        already_live_hash_seq_root_is_still_traversed(&store).await
    }

    async fn gc_check_deletion(store: &Store) -> TestResult {
        let temp_tag = store.add_bytes(b"foo".to_vec()).temp_tag().await?;
        let hash = temp_tag.hash();
        assert_eq!(store.get_bytes(hash).await?.as_ref(), b"foo");
        drop(temp_tag);
        let mut live = HashSet::new();
        gc_run_once(store, &mut live).await?;

        // check that `get_bytes` returns an error.
        let res = store.get_bytes(hash).await;
        assert!(res.is_err());
        assert!(matches!(
            res,
            Err(ExportBaoError::ExportBaoInner {
                source: EncodeError::Io(cause),
                ..
            }) if cause.kind() == io::ErrorKind::NotFound
        ));

        // check that `export_ranges` returns an error.
        let res = store
            .export_ranges(hash, RangeSet2::all())
            .concatenate()
            .await;
        assert!(res.is_err());
        assert!(matches!(
            res,
            Err(RequestError::Inner{
                source: crate::api::Error::Io(cause),
                ..
            }) if cause.kind() == io::ErrorKind::NotFound
        ));

        // check that `export_bao` returns an error.
        let res = store
            .export_bao(hash, ChunkRanges::all())
            .bao_to_vec()
            .await;
        assert!(res.is_err());
        println!("export_bao res {res:?}");
        assert!(matches!(
            res,
            Err(RequestError::Inner{
                source: crate::api::Error::Io(cause),
                ..
            }) if cause.kind() == io::ErrorKind::NotFound
        ));

        // check that `export` returns an error.
        let target = tempfile::NamedTempFile::new()?;
        let path = target.path();
        let res = store.export(hash, path).await;
        assert!(res.is_err());
        assert!(matches!(
            res,
            Err(RequestError::Inner{
                source: crate::api::Error::Io(cause),
                ..
            }) if cause.kind() == io::ErrorKind::NotFound
        ));
        Ok(())
    }
}
