// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use sui_protocol_config::ProtocolConfig;

use crate::{
    base_types::{dbg_addr, ExecutionDigests, ObjectID, ObjectRef, SuiAddress},
    committee::Committee,
    digests::TransactionDigest,
    effects::{TestEffectsBuilder, TransactionEffectsAPI},
    full_checkpoint_content::{CheckpointData, CheckpointTransaction},
    message_envelope::Message,
    messages_checkpoint::{CertifiedCheckpointSummary, CheckpointContents, CheckpointSummary},
    object::{Object, Owner},
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{SenderSignedData, Transaction, TransactionData, TransactionKind},
};

#[derive(Default)]
pub struct TestCheckpointDataBuilder {
    checkpoint: u64,
    epoch: u64,
    transactions: Vec<CheckpointTransaction>,

    next_transaction: Option<CpTransactionBuilder>,
    object_map: HashMap<ObjectID, Object>,
    gas_map: HashMap<SuiAddress, ObjectID>,
}

struct CpTransactionBuilder {
    sender: SuiAddress,
    gas: ObjectRef,
    created_objects: Vec<(ObjectID, Owner)>,
    mutated_object: Vec<(ObjectID, Owner)>,
    deleted_objects: Vec<ObjectID>,
}

impl CpTransactionBuilder {
    pub fn new(sender: SuiAddress, gas: ObjectRef) -> Self {
        Self {
            sender,
            gas,
            created_objects: vec![],
            mutated_object: vec![],
            deleted_objects: vec![],
        }
    }
}

impl TestCheckpointDataBuilder {
    pub fn new(checkpoint: u64) -> Self {
        Self {
            checkpoint,
            ..Default::default()
        }
    }

    pub fn with_epoch(mut self, epoch: u64) -> Self {
        self.epoch = epoch;
        self
    }

    pub fn start_transaction(mut self, sender_idx: u8) -> Self {
        assert!(self.next_transaction.is_none());
        let sender = dbg_addr(sender_idx);
        let gas_id = self.gas_map.entry(sender).or_insert_with(|| {
            let gas = Object::with_owner_for_testing(sender);
            let id = gas.id();
            self.object_map.insert(id, gas);
            id
        });
        let gas_ref = self
            .object_map
            .get(gas_id)
            .cloned()
            .unwrap()
            .compute_object_reference();
        self.next_transaction = Some(CpTransactionBuilder::new(sender, gas_ref));
        self
    }

    pub fn create_owned_object(mut self, object_idx: u64) -> Self {
        let tx_builder = self.next_transaction.as_mut().unwrap();
        let object_id = derive_object_id(object_idx);
        tx_builder
            .created_objects
            .push((object_id, Owner::AddressOwner(tx_builder.sender)));
        self
    }

    pub fn mutate_object(mut self, object_idx: u64) -> Self {
        let tx_builder = self.next_transaction.as_mut().unwrap();
        let object_id = derive_object_id(object_idx);
        assert!(self.object_map.contains_key(&object_id));
        tx_builder
            .mutated_object
            .push((object_id, Owner::AddressOwner(tx_builder.sender)));
        self
    }

    pub fn transfer_object(mut self, object_idx: u64, recipient_idx: u8) -> Self {
        let tx_builder = self.next_transaction.as_mut().unwrap();
        let object_id = derive_object_id(object_idx);
        assert!(self.object_map.contains_key(&object_id));
        tx_builder
            .mutated_object
            .push((object_id, Owner::AddressOwner(dbg_addr(recipient_idx))));
        self
    }

    pub fn delete_object(mut self, object_idx: u64) -> Self {
        let tx_builder = self.next_transaction.as_mut().unwrap();
        let object_id = derive_object_id(object_idx);
        assert!(self.object_map.contains_key(&object_id));
        tx_builder.deleted_objects.push(object_id);
        self
    }

    pub fn finish_transaction(mut self) -> Self {
        let CpTransactionBuilder {
            sender,
            gas,
            created_objects,
            mutated_object,
            deleted_objects,
        } = self.next_transaction.take().unwrap();
        let pt = ProgrammableTransactionBuilder::new().finish();
        let tx_data = TransactionData::new(
            TransactionKind::ProgrammableTransaction(pt),
            sender,
            gas,
            1,
            1,
        );
        let tx = Transaction::new(SenderSignedData::new(tx_data, vec![]));
        let effects = TestEffectsBuilder::new(tx.data())
            .with_created_objects(created_objects.clone())
            .with_mutated_objects(
                mutated_object
                    .clone()
                    .into_iter()
                    .map(|(id, owner)| (id, self.object_map.get(&id).unwrap().version(), owner)),
            )
            .with_deleted_objects(
                deleted_objects
                    .clone()
                    .into_iter()
                    .map(|id| (id, self.object_map.get(&id).unwrap().version())),
            )
            .build();
        let input_objects = vec![self.object_map.get(&gas.0).cloned().unwrap()];
        let output_objects: Vec<_> = created_objects
            .iter()
            .map(|(id, owner)| {
                Object::with_id_owner_version_for_testing(
                    *id,
                    effects.lamport_version(),
                    owner.clone(),
                )
            })
            .chain(mutated_object.iter().map(|(id, owner)| {
                Object::with_id_owner_version_for_testing(
                    *id,
                    effects.lamport_version(),
                    owner.clone(),
                )
            }))
            .chain(std::iter::once(Object::with_id_owner_version_for_testing(
                gas.0,
                effects.lamport_version(),
                Owner::AddressOwner(sender),
            )))
            .collect();
        self.object_map
            .extend(output_objects.iter().map(|o| (o.id(), o.clone())));
        self.transactions.push(CheckpointTransaction {
            transaction: tx,
            effects,
            events: None,
            input_objects,
            output_objects,
        });
        self
    }

    pub fn build(self) -> CheckpointData {
        assert!(self.next_transaction.is_none());
        let contents = CheckpointContents::new_with_digests_only_for_tests(
            self.transactions
                .iter()
                .map(|tx| ExecutionDigests::new(*tx.transaction.digest(), tx.effects.digest())),
        );
        let checkpoint_summary = CheckpointSummary::new(
            &ProtocolConfig::get_for_max_version_UNSAFE(),
            self.epoch,
            self.checkpoint,
            self.transactions.len() as u64,
            &contents,
            None,
            Default::default(),
            None,
            0,
            vec![],
        );
        let (committee, keys) = Committee::new_simple_test_committee();
        let checkpoint_cert = CertifiedCheckpointSummary::new_from_keypairs_for_testing(
            checkpoint_summary,
            &keys,
            &committee,
        );
        CheckpointData {
            checkpoint_summary: checkpoint_cert,
            checkpoint_contents: contents,
            transactions: self.transactions,
        }
    }
}

fn derive_object_id(object_idx: u64) -> ObjectID {
    ObjectID::derive_id(TransactionDigest::ZERO, object_idx)
}

#[cfg(test)]
mod tests {
    use crate::transaction::TransactionDataAPI;

    use super::*;
    #[test]
    fn test_basic_checkpoint_builder() {
        let checkpoint = TestCheckpointDataBuilder::new(1)
            .with_epoch(5)
            .start_transaction(0)
            .finish_transaction()
            .build();

        assert_eq!(*checkpoint.checkpoint_summary.sequence_number(), 1);
        assert_eq!(checkpoint.checkpoint_summary.epoch(), 5);
        assert_eq!(checkpoint.transactions.len(), 1);
        let tx = &checkpoint.transactions[0];
        assert_eq!(tx.transaction.sender_address(), dbg_addr(0));
        assert_eq!(tx.effects.mutated().len(), 1); // gas object
        assert_eq!(tx.effects.deleted().len(), 0);
        assert_eq!(tx.effects.created().len(), 0);
        assert_eq!(tx.input_objects.len(), 1);
        assert_eq!(tx.output_objects.len(), 1);
    }

    #[test]
    fn test_multiple_transactions() {
        let checkpoint = TestCheckpointDataBuilder::new(1)
            .start_transaction(0)
            .finish_transaction()
            .start_transaction(1)
            .finish_transaction()
            .start_transaction(2)
            .finish_transaction()
            .build();

        assert_eq!(checkpoint.transactions.len(), 3);

        // Verify transactions have different senders
        let senders: Vec<_> = checkpoint
            .transactions
            .iter()
            .map(|tx| tx.transaction.transaction_data().sender())
            .collect();
        assert_eq!(senders, vec![dbg_addr(0), dbg_addr(1), dbg_addr(2)]);
    }

    #[test]
    fn test_object_creation() {
        let checkpoint = TestCheckpointDataBuilder::new(1)
            .start_transaction(0)
            .create_owned_object(0)
            .finish_transaction()
            .build();

        let tx = &checkpoint.transactions[0];
        let created_obj_id = derive_object_id(0);

        // Verify object appears in output objects
        assert!(tx
            .output_objects
            .iter()
            .any(|obj| obj.id() == created_obj_id));

        // Verify effects show object creation
        assert!(tx
            .effects
            .created()
            .iter()
            .any(|((id, ..), owner)| *id == created_obj_id
                && owner.get_owner_address().unwrap() == dbg_addr(0)));
    }

    #[test]
    fn test_object_mutation() {
        let checkpoint = TestCheckpointDataBuilder::new(1)
            .start_transaction(0)
            .create_owned_object(0)
            .finish_transaction()
            .start_transaction(0)
            .mutate_object(0)
            .finish_transaction()
            .build();

        let tx = &checkpoint.transactions[1];
        let obj_id = derive_object_id(0);

        // Verify object appears in input and output objects
        assert!(tx.input_objects.iter().any(|obj| obj.id() == obj_id));
        assert!(tx.output_objects.iter().any(|obj| obj.id() == obj_id));

        // Verify effects show object mutation
        assert!(tx
            .effects
            .mutated()
            .iter()
            .any(|((id, ..), _)| *id == obj_id));
    }

    #[test]
    fn test_object_deletion() {
        let checkpoint = TestCheckpointDataBuilder::new(1)
            .start_transaction(0)
            .create_owned_object(0)
            .finish_transaction()
            .start_transaction(0)
            .delete_object(0)
            .finish_transaction()
            .build();

        let tx = &checkpoint.transactions[1];
        let obj_id = derive_object_id(0);

        // Verify object appears in input objects but not output
        assert!(tx.input_objects.iter().any(|obj| obj.id() == obj_id));
        assert!(!tx.output_objects.iter().any(|obj| obj.id() == obj_id));

        // Verify effects show object deletion
        assert!(tx.effects.deleted().iter().any(|(id, ..)| *id == obj_id));
    }

    #[test]
    fn test_object_transfer() {
        let checkpoint = TestCheckpointDataBuilder::new(1)
            .start_transaction(0)
            .create_owned_object(0)
            .finish_transaction()
            .start_transaction(1)
            .transfer_object(0, 1)
            .finish_transaction()
            .build();

        let tx = &checkpoint.transactions[1];
        let obj_id = derive_object_id(0);

        // Verify object appears in input and output objects
        assert!(tx.input_objects.iter().any(|obj| obj.id() == obj_id));
        assert!(tx.output_objects.iter().any(|obj| obj.id() == obj_id));

        // Verify effects show object transfer
        assert!(tx
            .effects
            .mutated()
            .iter()
            .any(|((id, ..), owner)| *id == obj_id
                && owner.get_owner_address().unwrap() == dbg_addr(1)));
    }
}
