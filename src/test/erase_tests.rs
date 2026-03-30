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
