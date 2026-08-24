# GC skips a live `HashSeq` root's children

Suggested upstream MR title:

> fix(gc): traverse collection roots even when their hash is already live

## Summary

The GC mark phase currently uses insertion into the hash-only `live` set as the
condition for traversing a format-bearing root:

```rust
if live.insert(hash) && !format.is_raw() {
    // traverse HashSeq children
}
```

These are separate facts. A collection root may already be live while its
children still need to be discovered. This happens when:

- the protection callback pre-seeds the root hash;
- the same bytes are rooted as both `Raw` and `HashSeq`, and the raw descriptor
  is visited first; or
- the root hash is already a member of another live collection.

In each case, `live.insert(hash)` returns `false`, the `HashSeq` is not
traversed, and resident children can be swept even though a live collection
root names them.

The bug is present on current upstream `main` and predates the Aster custody
changes. The reproduction below uses only the pre-existing GC API.

## Deterministic reproduction

Add this helper and the two store wrappers to `src/store/gc.rs`'s existing
`tests` module:

```rust
async fn already_live_hash_seq_root_still_marks_children(
    store: &Store,
) -> TestResult<()> {
    let blobs = store.blobs();

    // Store a child, then remove its temporary root so GC may collect it.
    let child_tag = blobs.add_slice(b"collection child").temp_tag().await?;
    let child = child_tag.hash();
    drop(child_tag);

    // Store a HashSeq naming that child, then retain it only through an
    // explicit HashSeq tag.
    let sequence: HashSeq = [child].into_iter().collect();
    let root_tag = blobs
        .add_bytes_with_opts(AddBytesOptions {
            data: sequence.into(),
            format: BlobFormat::HashSeq,
        })
        .temp_tag()
        .await?;
    let root = root_tag.hash();
    drop(root_tag);
    store
        .tags()
        .set("live-collection", HashAndFormat::hash_seq(root))
        .await?;

    // This is the deterministic equivalent of a raw alias being visited
    // first: the root hash is live before GC examines its HashSeq descriptor.
    let mut live = HashSet::from([root]);
    gc_run_once(store, &mut live).await?;

    assert!(store.has(root).await?, "the live root was unexpectedly swept");
    assert!(
        store.has(child).await?,
        "GC did not traverse an already-live HashSeq root and swept its child",
    );
    Ok(())
}

#[tokio::test]
#[cfg(feature = "fs-store")]
async fn already_live_hash_seq_root_still_marks_children_fs() -> TestResult {
    let testdir = tempfile::tempdir()?;
    let store = crate::store::fs::FsStore::load(testdir.path().join("db")).await?;
    already_live_hash_seq_root_still_marks_children(&store).await
}

#[tokio::test]
async fn already_live_hash_seq_root_still_marks_children_mem() -> TestResult {
    let store = crate::store::mem::MemStore::new();
    already_live_hash_seq_root_still_marks_children(&store).await
}
```

### Result before the fix

Both variants fail at the child assertion. The root survives because it was
already in `live`, but its child is absent after the sweep.

### Expected result

Both the root and child survive: root-hash liveness must not suppress
collection traversal.

## Minimal correction

Make insertion and traversal independent:

```rust
let _ = live.insert(hash);
if !format.is_raw() {
    // traverse HashSeq children
}
```

This also makes the result independent of iteration order when the same hash
has both `Raw` and `HashSeq` roots.

## Regression-test strength

Pre-seeding `live` is intentional. A test containing both root formats but
depending on `HashSet` iteration order would be probabilistic. Reverting the
fix to the joined condition above makes both deterministic store variants fail.

