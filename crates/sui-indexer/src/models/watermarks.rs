// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::schema::watermarks::{self};
use diesel::prelude::*;

/// Represents a row in the `watermarks` table.
#[derive(Queryable, Insertable, Default, QueryableByName)]
#[diesel(table_name = watermarks, primary_key(entity))]
pub struct StoredWatermark {
    /// The table name of group of tables governed by this watermark, i.e `epochs`, `checkpoints`,
    /// `transactions`.
    pub entity: String,
    /// Upper bound epoch range to enable per-entity epoch-level retention policy. Committer
    /// advances this along with `high`.
    pub epoch_hi: i64,
    /// Lower bound epoch range to enable per-entity epoch-level retention policy. Pruner advances
    /// this.
    pub epoch_lo: i64,
    pub checkpoint_hi: i64,
    /// The inclusive high watermark that the committer advances.
    pub hi: i64,
    /// The inclusive low watermark that the pruner advances. Data before this watermark is
    /// considered pruned.
    pub lo: i64,
    /// Pruner sets this, and uses this column to determine whether to prune or wait long enough
    /// that all in-flight reads complete or timeout before it acts on an updated watermark.
    pub timestamp_ms: i64,
    /// Pruner updates this, and uses this when recovering from a crash to determine where to
    /// continue pruning. Represents the latest watermark pruned, inclusive.
    pub pruned_lo: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum WatermarkEntity {
    Checkpoints,
    Events,
    EvLookup,
    // EventEmit,
    // EventSenders,
    // EventStruct,
    ObjectsHistory,
    Transactions,
    TxLookup,
    // TxAffectedAddresses,
    // TxAffectedObjects,
    // TxCalls,
    // TxChangedObjects,
    // TxDigests,
    // TxInputObjects,
    // TxKinds,
    // TxRecipients,
    // TxSenders,
}

pub struct Watermark {
    pub entity: WatermarkEntity,
    pub update: WatermarkUpdate,
}

pub enum WatermarkUpdate {
    /// The subset of fields that the committer updates.
    UpperBound { epoch_hi: u64, hi: u64 },
    /// The subset of fields that the pruner updates.
    LowerBound { epoch_lo: u64, lo: u64 },
    /// Pruner logs its progress to this field. Uses this when recovering from a crash to determine
    /// where to continue pruning. Represents the latest watermark pruned, inclusive.
    LowerBoundTail { pruned_lo: u64 },
}

#[derive(Eq, Hash, PartialEq)]
pub enum WatermarkUpdateType {
    UpperBound,
    LowerBound,
    LowerBoundTail,
}

#[derive(Debug)]
pub struct WatermarkRead {
    pub entity: WatermarkEntity,
    pub epoch_hi: u64,
    pub epoch_lo: u64,
    pub hi: u64,
    pub lo: u64,
    pub timestamp_ms: i64,
    /// Data at and below `pruned_lo` is considered pruned by the pruner.
    pub pruned_lo: Option<u64>,
}

impl WatermarkRead {
    /// Returns the inclusive high watermark that the reader should use.
    pub fn reader_hi(&self) -> u64 {
        self.hi
    }

    /// Returns the inclusive low watermark that the reader should use.
    pub fn reader_lo(&self) -> u64 {
        self.lo
    }

    /// Represents the first `unit` (checkpoint, tx, epoch) that has not yet been pruned. If
    /// `pruned_lo` is not set in db, default to 0. Otherwise, this is `pruned_lo + `.
    pub fn pruner_lo(&self) -> u64 {
        self.pruned_lo.map_or(0, |lo| lo.saturating_add(1))
    }

    pub fn map_to_bound(&self, cp: u64, tx: u64) -> u64 {
        match self.entity {
            WatermarkEntity::Checkpoints => cp,
            WatermarkEntity::ObjectsHistory => cp,
            WatermarkEntity::Events => tx,
            WatermarkEntity::EvLookup => tx,
            WatermarkEntity::Transactions => tx,
            WatermarkEntity::TxLookup => tx,
        }
    }
}

impl WatermarkEntity {
    pub fn as_str(&self) -> &'static str {
        match self {
            WatermarkEntity::Checkpoints => "checkpoints",
            WatermarkEntity::ObjectsHistory => "objects_history",
            WatermarkEntity::Events => "events",
            WatermarkEntity::EvLookup => "ev_lookup",
            WatermarkEntity::Transactions => "transactions",
            WatermarkEntity::TxLookup => "tx_lookup",
        }
    }

    pub fn from_str(entity: &str) -> Option<Self> {
        match entity {
            "transactions" => Some(WatermarkEntity::Transactions),
            "objects_history" => Some(WatermarkEntity::ObjectsHistory),
            "checkpoints" => Some(WatermarkEntity::Checkpoints),
            "events" => Some(WatermarkEntity::Events),
            "ev_lookup" => Some(WatermarkEntity::EvLookup),
            "tx_lookup" => Some(WatermarkEntity::TxLookup),
            _ => None,
        }
    }
}

impl Watermark {
    pub fn upper_bound(entity: WatermarkEntity, epoch_hi: u64, hi: u64) -> Self {
        Watermark {
            entity,
            update: WatermarkUpdate::UpperBound { epoch_hi, hi },
        }
    }

    pub fn lower_bound(entity: WatermarkEntity, epoch_lo: u64, lo: u64) -> Self {
        Watermark {
            entity,
            update: WatermarkUpdate::LowerBound { epoch_lo, lo },
        }
    }

    pub fn new_lower_bound_tail(entity: WatermarkEntity, pruned_lo: u64) -> Self {
        Watermark {
            entity,
            update: WatermarkUpdate::LowerBoundTail { pruned_lo },
        }
    }

    pub fn new_upper_bounds(epoch_hi: u64, cp_hi: u64, tx_hi: u64) -> Vec<Watermark> {
        vec![
            Watermark::upper_bound(WatermarkEntity::Checkpoints, epoch_hi, cp_hi),
            Watermark::upper_bound(WatermarkEntity::ObjectsHistory, epoch_hi, cp_hi),
            Watermark::upper_bound(WatermarkEntity::Events, epoch_hi, tx_hi),
            Watermark::upper_bound(WatermarkEntity::EvLookup, epoch_hi, tx_hi),
            Watermark::upper_bound(WatermarkEntity::Transactions, epoch_hi, tx_hi),
            Watermark::upper_bound(WatermarkEntity::TxLookup, epoch_hi, tx_hi),
        ]
    }

    pub fn new_lower_bounds(epoch_lo: u64, cp_lo: u64, tx_lo: u64) -> Vec<Watermark> {
        vec![
            Watermark::lower_bound(WatermarkEntity::Checkpoints, epoch_lo, cp_lo),
            Watermark::lower_bound(WatermarkEntity::ObjectsHistory, epoch_lo, cp_lo),
            Watermark::lower_bound(WatermarkEntity::Events, epoch_lo, tx_lo),
            Watermark::lower_bound(WatermarkEntity::EvLookup, epoch_lo, tx_lo),
            Watermark::lower_bound(WatermarkEntity::Transactions, epoch_lo, tx_lo),
            Watermark::lower_bound(WatermarkEntity::TxLookup, epoch_lo, tx_lo),
        ]
    }

    pub fn update_type(&self) -> WatermarkUpdateType {
        match self.update {
            WatermarkUpdate::UpperBound { .. } => WatermarkUpdateType::UpperBound,
            WatermarkUpdate::LowerBound { .. } => WatermarkUpdateType::LowerBound,
            WatermarkUpdate::LowerBoundTail { .. } => WatermarkUpdateType::LowerBoundTail,
        }
    }
}

impl From<Watermark> for StoredWatermark {
    fn from(watermark: Watermark) -> Self {
        match watermark.update {
            WatermarkUpdate::UpperBound { epoch_hi, hi } => StoredWatermark {
                entity: watermark.entity.as_str().to_string(),
                epoch_hi: epoch_hi as i64,
                hi: hi as i64,
                ..StoredWatermark::default()
            },
            WatermarkUpdate::LowerBound { epoch_lo, lo } => StoredWatermark {
                entity: watermark.entity.as_str().to_string(),
                epoch_hi: epoch_lo as i64,
                epoch_lo: epoch_lo as i64,
                hi: lo as i64,
                lo: lo as i64,
                ..StoredWatermark::default()
            },
            WatermarkUpdate::LowerBoundTail { pruned_lo } => StoredWatermark {
                entity: watermark.entity.as_str().to_string(),
                pruned_lo: Some(pruned_lo as i64),
                ..StoredWatermark::default()
            },
        }
    }
}

impl From<StoredWatermark> for WatermarkRead {
    fn from(watermark: StoredWatermark) -> Self {
        let entity = WatermarkEntity::from_str(&watermark.entity).unwrap();

        WatermarkRead {
            entity,
            epoch_hi: watermark.epoch_hi as u64,
            epoch_lo: watermark.epoch_lo as u64,
            hi: watermark.hi as u64,
            lo: watermark.lo as u64,
            timestamp_ms: watermark.timestamp_ms,
            pruned_lo: watermark.pruned_lo.map(|x| x as u64),
        }
    }
}
