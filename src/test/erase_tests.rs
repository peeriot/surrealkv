//! Tests for `erase_with_options`.
//!
//! Erase removes the version of a key at a specific timestamp. Reads at or
//! after that timestamp must fall through to the previous version (if any) for
//! both point reads and range scans.

use tempdir::TempDir;
use test_log::test;

use crate::test::collect_transaction_all;
use crate::{LSMIterator, Options, TreeBuilder, WriteOptions};

fn create_versioned_store() -> (crate::lsm::Tree, TempDir) {
	let temp_dir = TempDir::new("test").unwrap();
	let opts = Options::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_versioning(true, 0);
	let tree = TreeBuilder::with_options(opts).build().unwrap();
	(tree, temp_dir)
}

#[test(tokio::test)]
async fn erase_only_version_range_scan_skips_key() {
	let (store, _temp_dir) = create_versioned_store();

	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.erase_with_options(b"key1", &WriteOptions::new().with_timestamp(Some(100))).unwrap();
		tx.commit().await.unwrap();
	}

	let tx = store.begin().unwrap();
	let mut iter = tx.range(b"key0", b"key9").unwrap();
	let results = collect_transaction_all(&mut iter).unwrap();

	assert!(
		results.is_empty(),
		"erasing the only version should remove the key from range scans, got {results:?}"
	);
}

#[test(tokio::test)]
async fn erase_only_version_range_scan_after_flush() {
	let (store, _temp_dir) = create_versioned_store();

	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.erase_with_options(b"key1", &WriteOptions::new().with_timestamp(Some(100))).unwrap();
		tx.commit().await.unwrap();
	}

	store.flush().unwrap();

	let tx = store.begin().unwrap();
	let mut iter = tx.range(b"key0", b"key9").unwrap();
	let results = collect_transaction_all(&mut iter).unwrap();

	assert!(
		results.is_empty(),
		"erasing the only version should remove the key from range scans even after flush, got {results:?}"
	);
}

#[test(tokio::test)]
async fn erase_newest_version_range_scan_returns_previous() {
	let (store, _temp_dir) = create_versioned_store();

	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1_v1", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1_v2", 200).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.erase_with_options(b"key1", &WriteOptions::new().with_timestamp(Some(200))).unwrap();
		tx.commit().await.unwrap();
	}

	let tx = store.begin().unwrap();
	let mut iter = tx.range(b"key0", b"key9").unwrap();
	let results = collect_transaction_all(&mut iter).unwrap();

	assert_eq!(results.len(), 1, "should return one (previous) version, got {results:?}");
	assert_eq!(&results[0].0, b"key1");
	assert_eq!(&results[0].1, b"value1_v1");
}

#[test(tokio::test)]
async fn erase_middle_version_range_scan_returns_newest() {
	let (store, _temp_dir) = create_versioned_store();

	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1_v1", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1_v2", 200).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1_v3", 300).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.erase_with_options(b"key1", &WriteOptions::new().with_timestamp(Some(200))).unwrap();
		tx.commit().await.unwrap();
	}

	let tx = store.begin().unwrap();
	let mut iter = tx.range(b"key0", b"key9").unwrap();
	let results = collect_transaction_all(&mut iter).unwrap();

	assert_eq!(results.len(), 1, "should return the newest non-erased version, got {results:?}");
	assert_eq!(&results[0].0, b"key1");
	assert_eq!(&results[0].1, b"value1_v3");
}

#[test(tokio::test)]
async fn erase_only_version_range_scan_reverse() {
	let (store, _temp_dir) = create_versioned_store();

	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.erase_with_options(b"key1", &WriteOptions::new().with_timestamp(Some(100))).unwrap();
		tx.commit().await.unwrap();
	}

	let tx = store.begin().unwrap();
	let mut iter = tx.range(b"key0", b"key9").unwrap();

	iter.seek_last().unwrap();
	let mut results = Vec::new();
	while iter.valid() {
		let key = iter.key().user_key().to_vec();
		let value = iter.value().unwrap();
		results.push((key, value));
		if !iter.prev().unwrap() {
			break;
		}
	}

	assert!(
		results.is_empty(),
		"reverse scan should also skip erased-only keys, got {results:?}"
	);
}

#[test(tokio::test)]
async fn erase_only_version_point_read_returns_none() {
	let (store, _temp_dir) = create_versioned_store();

	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.erase_with_options(b"key1", &WriteOptions::new().with_timestamp(Some(100))).unwrap();
		tx.commit().await.unwrap();
	}

	let tx = store.begin().unwrap();
	let result = tx.get(b"key1").unwrap();
	assert_eq!(result, None, "point read of erased-only key should be None");
}

#[test(tokio::test)]
async fn erase_one_key_does_not_affect_neighbours() {
	let (store, _temp_dir) = create_versioned_store();

	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v1", 100).unwrap();
		tx.set_at(b"key2", b"v2", 100).unwrap();
		tx.set_at(b"key3", b"v3", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.erase_with_options(b"key2", &WriteOptions::new().with_timestamp(Some(100))).unwrap();
		tx.commit().await.unwrap();
	}

	let tx = store.begin().unwrap();
	let mut iter = tx.range(b"key0", b"key9").unwrap();
	let results = collect_transaction_all(&mut iter).unwrap();

	assert_eq!(results.len(), 2);
	assert_eq!(&results[0].0, b"key1");
	assert_eq!(&results[0].1, b"v1");
	assert_eq!(&results[1].0, b"key3");
	assert_eq!(&results[1].1, b"v3");
}

#[test(tokio::test)]
async fn erase_then_reinsert_same_version_same_tx() {
	let (store, _temp_dir) = create_versioned_store();

	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"orig", 100).unwrap();
		tx.commit().await.unwrap();
	}
	// Restore-style: in one tx, erase the version then write a new value at the
	// same timestamp. The new value must win.
	{
		let mut tx = store.begin().unwrap();
		tx.erase_with_options(b"key1", &WriteOptions::new().with_timestamp(Some(100))).unwrap();
		tx.set_at(b"key1", b"restored", 100).unwrap();
		tx.commit().await.unwrap();
	}

	let tx = store.begin().unwrap();
	assert_eq!(
		tx.get_at(b"key1", u64::MAX).unwrap().as_deref(),
		Some(&b"restored"[..]),
		"value re-written at an erased version must be visible",
	);
}

#[test(tokio::test)]
async fn erase_newer_version_falls_back_to_reasserted_older() {
	let (store, _temp_dir) = create_versioned_store();

	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"A", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"B", 200).unwrap();
		tx.commit().await.unwrap();
	}
	// Roll back: erase the newer version (200) and re-assert the older one (100).
	{
		let mut tx = store.begin().unwrap();
		tx.erase_with_options(b"key1", &WriteOptions::new().with_timestamp(Some(200))).unwrap();
		tx.set_at(b"key1", b"A", 100).unwrap();
		tx.commit().await.unwrap();
	}

	let tx = store.begin().unwrap();
	assert_eq!(
		tx.get_at(b"key1", u64::MAX).unwrap().as_deref(),
		Some(&b"A"[..]),
		"erasing the newer version must fall back to the re-asserted older value",
	);
}

#[test(tokio::test)]
async fn soft_delete_same_version_as_committed_value_wins() {
	let (store, _temp_dir) = create_versioned_store();

	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"val", 100).unwrap();
		tx.commit().await.unwrap();
	}
	// Separate tx (higher seq) soft-deletes at the SAME timestamp.
	{
		let mut tx = store.begin().unwrap();
		tx.soft_delete_with_options(b"key1", &WriteOptions::new().with_timestamp(Some(100))).unwrap();
		tx.commit().await.unwrap();
	}

	let tx = store.begin().unwrap();
	assert_eq!(
		tx.get_at(b"key1", u64::MAX).unwrap(),
		None,
		"a later soft-delete at the same timestamp must win over the committed value",
	);
}

#[test(tokio::test)]
async fn history_ts_range_with_reinserted_old_version() {
	let (store, _temp_dir) = create_versioned_store();

	// x@100=A, x@200=B, then re-insert x@100=A2 (newer seq, older ts).
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"x", b"A", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"x", b"B", 200).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"x", b"A2", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// History scoped to ts=200 must still surface x@200.
	let tx = store.begin().unwrap();
	let opts = crate::HistoryOptions::new().with_tombstones(true).with_ts_range(200, 200);
	let mut it = tx.history_with_options(&b"x"[..], &b"y"[..], &opts).unwrap();
	let mut found = vec![];
	if it.seek_first().unwrap() {
		while it.valid() {
			found.push((it.key().user_key().to_vec(), it.key().timestamp()));
			it.next().unwrap();
		}
	}
	assert_eq!(found.len(), 1, "ts_range [200,200] should surface x@200, got {found:?}");
}

#[test(tokio::test)]
async fn newest_write_at_same_timestamp_wins_across_sources() {
	let (store, _temp_dir) = create_versioned_store();
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"k", b"A", 100).unwrap();
		tx.commit().await.unwrap();
	}
	store.flush().unwrap(); // A lands in an sstable
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"k", b"B", 100).unwrap();
		tx.commit().await.unwrap();
	}
	let tx = store.begin().unwrap();
	assert_eq!(tx.get_at(b"k", u64::MAX).unwrap().as_deref(), Some(&b"B"[..]),
		"newest write at the same timestamp should win");
}
