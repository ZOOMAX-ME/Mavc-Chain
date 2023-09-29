// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use diesel::prelude::*;
use move_bytecode_utils::module_cache::GetModule;
use sui_json_rpc_types::BalanceChange;
use sui_json_rpc_types::ObjectChange;
use sui_json_rpc_types::SuiTransactionBlock;
use sui_json_rpc_types::SuiTransactionBlockEffects;
use sui_json_rpc_types::SuiTransactionBlockEvents;
use sui_json_rpc_types::SuiTransactionBlockResponse;
use sui_types::digests::TransactionDigest;
use sui_types::effects::TransactionEffects;
use sui_types::effects::TransactionEvents;
use sui_types::event::Event;
use sui_types::transaction::SenderSignedData;

use crate::errors::IndexerError;
use crate::schema_v2::transactions;
use crate::types_v2::IndexedObjectChange;
use crate::types_v2::IndexedTransaction;
use crate::types_v2::IndexerResult;

use std::str::FromStr;

#[derive(Clone, Debug, Queryable, Insertable, QueryableByName)]
#[diesel(table_name = transactions)]
pub struct StoredTransaction {
    pub tx_sequence_number: i64,
    pub transaction_digest: String,
    pub raw_transaction: Vec<u8>,
    pub raw_effects: Vec<u8>,
    pub checkpoint_sequence_number: i64,
    pub timestamp_ms: i64,
    // pub object_changes: Vec<Option<Vec<u8>>>,
    pub object_changes: Vec<u8>,
    // pub balance_changes: Vec<Option<Vec<u8>>>,
    pub balance_changes: Vec<u8>,
    // pub events: Vec<Option<Vec<u8>>>,
    pub events: Vec<u8>,
    pub transaction_kind: i16,
}

impl From<&IndexedTransaction> for StoredTransaction {
    fn from(tx: &IndexedTransaction) -> Self {
        let object_changes: Vec<Option<Vec<u8>>> = tx
            .object_changes
            .iter()
            .map(|oc| Some(bcs::to_bytes(&oc).unwrap()))
            .collect();
        let flattened_object_changes: Vec<u8> = object_changes.into_iter().filter_map(|opt| opt).flatten().collect();

        let balance_changes: Vec<Option<Vec<u8>>> = tx
            .balance_change
            .iter()
            .map(|bc| Some(bcs::to_bytes(&bc).unwrap()))
            .collect();
        let flattened_balance_changes: Vec<u8> = balance_changes.into_iter().filter_map(|opt| opt).flatten().collect();

        let events: Vec<Option<Vec<u8>>> = tx
            .events
            .iter()
            .map(|e| Some(bcs::to_bytes(&e).unwrap()))
            .collect();
        let flattened_events: Vec<u8> = events.into_iter().filter_map(|opt| opt).flatten().collect();

        StoredTransaction {
            tx_sequence_number: tx.tx_sequence_number as i64,
            transaction_digest: tx.tx_digest.to_string(),
            raw_transaction: bcs::to_bytes(&tx.sender_signed_data).unwrap(),
            raw_effects: bcs::to_bytes(&tx.effects).unwrap(),
            checkpoint_sequence_number: tx.checkpoint_sequence_number as i64,
            object_changes: flattened_object_changes,
            balance_changes: flattened_balance_changes,
            events: flattened_events,
            transaction_kind: tx.transaction_kind.clone() as i16,
            timestamp_ms: tx.timestamp_ms as i64,
        }
    }
}

impl StoredTransaction {
    pub fn try_into_sui_transaction_block_response(
        self,
        module: &impl GetModule,
    ) -> IndexerResult<SuiTransactionBlockResponse> {
        let tx_digest = TransactionDigest::from_str(&self.transaction_digest).map_err(|e| {
            IndexerError::PersistentStorageDataCorruptionError(format!(
                "Can't convert {:?} as tx_digest. Error: {e}",
                self.transaction_digest
            ))
        })?;
        let sender_signed_data: SenderSignedData =
            bcs::from_bytes(&self.raw_transaction).map_err(|e| {
                IndexerError::PersistentStorageDataCorruptionError(format!(
                    "Can't convert raw_transaction of {} into SenderSignedData. Error: {e}",
                    tx_digest
                ))
            })?;
        let tx_block = SuiTransactionBlock::try_from(sender_signed_data, module)?;
        let effects: TransactionEffects = bcs::from_bytes(&self.raw_effects).map_err(|e| {
            IndexerError::PersistentStorageDataCorruptionError(format!(
                "Can't convert raw_effects of {} into TransactionEffects. Error: {e}",
                tx_digest
            ))
        })?;
        let effects = SuiTransactionBlockEffects::try_from(effects)?;

        // let parsed_events: Vec<Option<Vec<u8>>> = serde_json::from_value(self.events.clone())
        //     .map_err(|e| {
        //         IndexerError::SerdeError(format!(
        //             "Failed to parse transaction events: {:?}, error: {}",
        //             self.events, e
        //         ))
        //     })?;
        let parsed_events: Vec<Option<Vec<u8>>> = vec![];
        let events = parsed_events
            .into_iter()
            .map(|event| match event {
                Some(event) => {
                    let event: Event = bcs::from_bytes(&event).map_err(|e| {
                        IndexerError::PersistentStorageDataCorruptionError(format!(
                            "Can't convert event bytes into Event. tx_digest={:?} Error: {e}",
                            tx_digest
                        ))
                    })?;
                    Ok(event)
                }
                None => Err(IndexerError::PersistentStorageDataCorruptionError(format!(
                    "Event should not be null, tx_digest={:?}",
                    tx_digest
                ))),
            })
            .collect::<Result<Vec<Event>, IndexerError>>()?;
        let timestamp = self.timestamp_ms as u64;
        let tx_events = TransactionEvents { data: events };
        let tx_events =
            SuiTransactionBlockEvents::try_from(tx_events, tx_digest, Some(timestamp), module)?;

        // let parsed_object_changes: Vec<Option<Vec<u8>>> =
        //     serde_json::from_value(self.object_changes.clone()).map_err(|e| {
        //         IndexerError::SerdeError(format!(
        //             "Failed to parse object changes: {:?}, error: {}",
        //             self.object_changes, e
        //         ))
        //     })?;
        let parsed_object_changes: Vec<Option<Vec<u8>>> = vec![];
        let object_changes = parsed_object_changes.into_iter().map(|object_change| {
            match object_change {
                Some(object_change) => {
                    let object_change: IndexedObjectChange = bcs::from_bytes(&object_change)
                        .map_err(|e| IndexerError::PersistentStorageDataCorruptionError(
                            format!("Can't convert object_change bytes into IndexedObjectChange. tx_digest={:?} Error: {e}", tx_digest)
                        ))?;
                    Ok(ObjectChange::from(object_change))
                }
                None => Err(IndexerError::PersistentStorageDataCorruptionError(format!("object_change should not be null, tx_digest={:?}", tx_digest))),
            }
        }).collect::<Result<Vec<ObjectChange>, IndexerError>>()?;

        // let parsed_balance_changes: Vec<Option<Vec<u8>>> =
        //     serde_json::from_value(self.balance_changes.clone()).map_err(|e| {
        //         IndexerError::SerdeError(format!(
        //             "Failed to parse balance changes: {:?}, error: {}",
        //             self.balance_changes, e
        //         ))
        //     })?;
        let parsed_balance_changes: Vec<Option<Vec<u8>>> = vec![];
        let balance_changes = parsed_balance_changes.into_iter().map(|balance_change| {
            match balance_change {
                Some(balance_change) => {
                    let balance_change: BalanceChange = bcs::from_bytes(&balance_change)
                        .map_err(|e| IndexerError::PersistentStorageDataCorruptionError(
                            format!("Can't convert balance_change bytes into BalanceChange. tx_digest={:?} Error: {e}", tx_digest)
                        ))?;
                    Ok(balance_change)
                }
                None => Err(IndexerError::PersistentStorageDataCorruptionError(format!("object_change should not be null, tx_digest={:?}", tx_digest))),
            }
        }).collect::<Result<Vec<BalanceChange>, IndexerError>>()?;

        Ok(SuiTransactionBlockResponse {
            digest: tx_digest,
            transaction: Some(tx_block),
            raw_transaction: self.raw_transaction,
            effects: Some(effects),
            events: Some(tx_events),
            object_changes: Some(object_changes),
            balance_changes: Some(balance_changes),
            timestamp_ms: Some(timestamp),
            checkpoint: Some(self.checkpoint_sequence_number as u64),
            confirmed_local_execution: None,
            errors: vec![],
        })
    }
}
