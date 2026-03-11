use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::Path;

use integer_encoding::{VarInt, VarIntWriter};

use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::wal::reader::Reader;
use crate::wal::{get_segment_range, list_segment_ids, Error as WalError, SegmentRef};

const TXN_WAL_MAGIC: u8 = 0xF3;
const TXN_WAL_VERSION: u8 = 2;
const LEGACY_PREPARE_VERSION: u8 = 1;

const RECORD_KIND_PREPARE: u8 = 1;
const RECORD_KIND_DECISION: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TxnDecision {
	Commit = 1,
	Abort = 2,
}

impl TxnDecision {
	fn from_u8(value: u8) -> Result<Self> {
		match value {
			1 => Ok(Self::Commit),
			2 => Ok(Self::Abort),
			_ => Err(Error::InvalidBatchRecord),
		}
	}
}

#[derive(Clone)]
pub(crate) struct PrepareWalRecord {
	pub(crate) txn_id: u64,
	pub(crate) should_sync: bool,
	pub(crate) batch: Batch,
}

impl PrepareWalRecord {
	pub(crate) fn new(txn_id: u64, should_sync: bool, batch: Batch) -> Self {
		Self {
			txn_id,
			should_sync,
			batch,
		}
	}

	pub(crate) fn encode(&self) -> Result<Vec<u8>> {
		let batch_bytes = self.batch.encode()?;
		let mut encoded = Vec::with_capacity(24 + batch_bytes.len());
		encoded.push(TXN_WAL_MAGIC);
		encoded.push(TXN_WAL_VERSION);
		encoded.push(RECORD_KIND_PREPARE);
		encoded.write_varint(self.txn_id)?;
		encoded.push(u8::from(self.should_sync));
		encoded.write_varint(batch_bytes.len() as u64)?;
		encoded.extend_from_slice(&batch_bytes);
		Ok(encoded)
	}
}

#[derive(Clone)]
pub(crate) struct DecisionWalRecord {
	pub(crate) txn_id: u64,
	pub(crate) decision: TxnDecision,
	pub(crate) committed_batch: Option<Batch>,
}

impl DecisionWalRecord {
	pub(crate) fn new(txn_id: u64, decision: TxnDecision, committed_batch: Option<Batch>) -> Self {
		let committed_batch = if decision == TxnDecision::Commit {
			committed_batch
		} else {
			None
		};

		Self {
			txn_id,
			decision,
			committed_batch,
		}
	}

	pub(crate) fn encode(&self) -> Result<Vec<u8>> {
		let mut encoded = Vec::with_capacity(16);
		encoded.push(TXN_WAL_MAGIC);
		encoded.push(TXN_WAL_VERSION);
		encoded.push(RECORD_KIND_DECISION);
		encoded.write_varint(self.txn_id)?;
		encoded.push(self.decision as u8);

		match &self.committed_batch {
			Some(batch) if self.decision == TxnDecision::Commit => {
				let batch_bytes = batch.encode()?;
				encoded.write_varint(batch_bytes.len() as u64)?;
				encoded.extend_from_slice(&batch_bytes);
			}
			_ => {
				encoded.write_varint(0u64)?;
			}
		}

		Ok(encoded)
	}
}

#[derive(Clone)]
pub(crate) struct RecoveredPreparedTransaction {
	pub(crate) txn_id: u64,
	pub(crate) should_sync: bool,
	pub(crate) batch: Batch,
	pub(crate) wal_number: u64,
}

pub(crate) enum TxnWalRecord {
	Prepare(PrepareWalRecord),
	Decision(DecisionWalRecord),
}

impl TxnWalRecord {
	pub(crate) fn is_txn_record(data: &[u8]) -> bool {
		matches!(data.first(), Some(&TXN_WAL_MAGIC))
	}

	pub(crate) fn decode(data: &[u8]) -> Result<Self> {
		if data.len() < 2 {
			return Err(Error::InvalidBatchRecord);
		}
		if data[0] != TXN_WAL_MAGIC {
			return Err(Error::InvalidBatchRecord);
		}

		match data[1] {
			LEGACY_PREPARE_VERSION => decode_legacy_prepare_record(data),
			TXN_WAL_VERSION => decode_v2_record(data),
			_ => Err(Error::InvalidBatchRecord),
		}
	}
}

fn decode_legacy_prepare_record(data: &[u8]) -> Result<TxnWalRecord> {
	let mut pos = 2;
	let (txn_id, bytes_read) = u64::decode_var(&data[pos..]).ok_or(Error::InvalidBatchRecord)?;
	pos += bytes_read;
	let (batch_len, bytes_read) = u64::decode_var(&data[pos..]).ok_or(Error::InvalidBatchRecord)?;
	pos += bytes_read;
	let batch_len = batch_len as usize;
	if pos + batch_len > data.len() {
		return Err(Error::InvalidBatchRecord);
	}
	let batch = Batch::decode(&data[pos..pos + batch_len])?;
	pos += batch_len;
	if pos != data.len() {
		return Err(Error::InvalidBatchRecord);
	}

	Ok(TxnWalRecord::Prepare(PrepareWalRecord {
		txn_id,
		// Legacy records had no explicit flag; default to eventual.
		should_sync: false,
		batch,
	}))
}

fn decode_v2_record(data: &[u8]) -> Result<TxnWalRecord> {
	if data.len() < 3 {
		return Err(Error::InvalidBatchRecord);
	}

	let record_kind = data[2];
	let mut pos = 3;

	match record_kind {
		RECORD_KIND_PREPARE => {
			let (txn_id, bytes_read) =
				u64::decode_var(&data[pos..]).ok_or(Error::InvalidBatchRecord)?;
			pos += bytes_read;

			let should_sync = match data.get(pos) {
				Some(v) => *v != 0,
				None => return Err(Error::InvalidBatchRecord),
			};
			pos += 1;

			let (batch_len, bytes_read) =
				u64::decode_var(&data[pos..]).ok_or(Error::InvalidBatchRecord)?;
			pos += bytes_read;
			let batch_len = batch_len as usize;
			if pos + batch_len > data.len() {
				return Err(Error::InvalidBatchRecord);
			}
			let batch = Batch::decode(&data[pos..pos + batch_len])?;
			pos += batch_len;
			if pos != data.len() {
				return Err(Error::InvalidBatchRecord);
			}

			Ok(TxnWalRecord::Prepare(PrepareWalRecord {
				txn_id,
				should_sync,
				batch,
			}))
		}
		RECORD_KIND_DECISION => {
			let (txn_id, bytes_read) =
				u64::decode_var(&data[pos..]).ok_or(Error::InvalidBatchRecord)?;
			pos += bytes_read;
			let decision = TxnDecision::from_u8(*data.get(pos).ok_or(Error::InvalidBatchRecord)?)?;
			pos += 1;

			// Legacy decision format ended immediately after decision byte.
			if pos == data.len() {
				return Ok(TxnWalRecord::Decision(DecisionWalRecord {
					txn_id,
					decision,
					committed_batch: None,
				}));
			}

			let (batch_len, bytes_read) =
				u64::decode_var(&data[pos..]).ok_or(Error::InvalidBatchRecord)?;
			pos += bytes_read;
			let batch_len = batch_len as usize;
			let committed_batch = if batch_len == 0 {
				None
			} else {
				if decision != TxnDecision::Commit {
					return Err(Error::InvalidBatchRecord);
				}
				if pos + batch_len > data.len() {
					return Err(Error::InvalidBatchRecord);
				}
				let batch = Batch::decode(&data[pos..pos + batch_len])?;
				pos += batch_len;
				Some(batch)
			};

			if pos != data.len() {
				return Err(Error::InvalidBatchRecord);
			}

			Ok(TxnWalRecord::Decision(DecisionWalRecord {
				txn_id,
				decision,
				committed_batch,
			}))
		}
		_ => Err(Error::InvalidBatchRecord),
	}
}

/// Result of scanning WAL for prepared transactions.
pub(crate) struct RecoveredPreparedTransactions {
	/// Unresolved prepared transactions (no commit/abort decision found).
	pub(crate) transactions: Vec<RecoveredPreparedTransaction>,
	/// Highest txn_id seen across ALL 2PC records (prepare + decision), including
	/// resolved ones. The caller MUST advance `next_txn_id` past this value to
	/// prevent id reuse after restart, which would cause a later prepare to be
	/// shadowed by a stale decision record for the same id.
	pub(crate) max_txn_id: u64,
}

pub(crate) fn recover_prepared_transactions(
	wal_dir: &Path,
	min_wal_number: u64,
) -> Result<RecoveredPreparedTransactions> {
	let empty = RecoveredPreparedTransactions {
		transactions: Vec::new(),
		max_txn_id: 0,
	};

	if !wal_dir.exists() || list_segment_ids(wal_dir, Some("wal"))?.is_empty() {
		return Ok(empty);
	}

	let (first, last) = match get_segment_range(wal_dir, Some("wal")) {
		Ok(range) => range,
		Err(WalError::IO(ref io_err)) if io_err.kind() == std::io::ErrorKind::NotFound => {
			return Ok(empty);
		}
		Err(e) => return Err(e.into()),
	};

	if first > last {
		return Ok(empty);
	}

	let start_segment = std::cmp::max(first, min_wal_number);
	if start_segment > last {
		return Ok(empty);
	}

	let all_segments = SegmentRef::read_segments_from_directory(wal_dir, Some("wal"))?;
	let mut prepared: HashMap<u64, RecoveredPreparedTransaction> = HashMap::new();
	let mut resolved: HashSet<u64> = HashSet::new();
	let mut max_txn_id: u64 = 0;

	for segment_id in start_segment..=last {
		let segment = match all_segments.iter().find(|seg| seg.id == segment_id) {
			Some(seg) => seg,
			None => continue,
		};

		let file = File::open(&segment.file_path)?;
		let mut reader = Reader::new(file);
		let mut last_valid_offset = 0usize;

		loop {
			match reader.read() {
				Ok((record_data, offset)) => {
					last_valid_offset = offset as usize;

					if !TxnWalRecord::is_txn_record(record_data) {
						continue;
					}

					match TxnWalRecord::decode(record_data)? {
						TxnWalRecord::Prepare(rec) => {
							if rec.txn_id > max_txn_id {
								max_txn_id = rec.txn_id;
							}
							if resolved.contains(&rec.txn_id) {
								continue;
							}
							prepared.insert(
								rec.txn_id,
								RecoveredPreparedTransaction {
									txn_id: rec.txn_id,
									should_sync: rec.should_sync,
									batch: rec.batch,
									wal_number: segment_id,
								},
							);
						}
						TxnWalRecord::Decision(decision) => {
							if decision.txn_id > max_txn_id {
								max_txn_id = decision.txn_id;
							}
							resolved.insert(decision.txn_id);
							prepared.remove(&decision.txn_id);
						}
					}
				}
				Err(WalError::Corruption(err)) => {
					return Err(Error::wal_corruption(
						segment_id as usize,
						last_valid_offset,
						format!("Corrupted WAL record: {}", err),
					));
				}
				Err(WalError::IO(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
					break;
				}
				Err(err) => return Err(err.into()),
			}
		}
	}

	let mut out: Vec<_> = prepared.into_values().collect();
	out.sort_by_key(|prepared| prepared.txn_id);
	Ok(RecoveredPreparedTransactions {
		transactions: out,
		max_txn_id,
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::InternalKeyKind;

	#[test]
	fn test_txn_prepare_record_roundtrip() {
		let mut batch = Batch::new(42);
		batch.add_record(InternalKeyKind::Set, b"k1".to_vec(), Some(b"v1".to_vec()), 123).unwrap();

		let rec = PrepareWalRecord::new(7, true, batch.clone());
		let encoded = rec.encode().unwrap();
		assert!(TxnWalRecord::is_txn_record(&encoded));

		let TxnWalRecord::Prepare(decoded) = TxnWalRecord::decode(&encoded).unwrap() else {
			panic!("expected prepare record");
		};
		assert_eq!(decoded.txn_id, 7);
		assert!(decoded.should_sync);
		assert_eq!(decoded.batch.count(), batch.count());
		assert_eq!(decoded.batch.starting_seq_num, batch.starting_seq_num);
	}

	#[test]
	fn test_txn_decision_record_roundtrip() {
		let mut batch = Batch::new(42);
		batch.add_record(InternalKeyKind::Set, b"k1".to_vec(), Some(b"v1".to_vec()), 5).unwrap();
		let rec = DecisionWalRecord::new(99, TxnDecision::Commit, Some(batch.clone()));
		let encoded = rec.encode().unwrap();
		let TxnWalRecord::Decision(decoded) = TxnWalRecord::decode(&encoded).unwrap() else {
			panic!("expected decision record");
		};
		assert_eq!(decoded.txn_id, 99);
		assert_eq!(decoded.decision, TxnDecision::Commit);
		let committed_batch = decoded.committed_batch.expect("missing committed batch");
		assert_eq!(committed_batch.count(), batch.count());
		assert_eq!(committed_batch.starting_seq_num, batch.starting_seq_num);
	}
}
