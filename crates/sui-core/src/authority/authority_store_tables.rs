// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::authority::authority_store::LockDetailsWrapperDeprecated;
use serde::{Deserialize, Serialize};
use std::path::Path;
use sui_types::accumulator::Accumulator;
use sui_types::base_types::SequenceNumber;
use sui_types::digests::TransactionEventsDigest;
use sui_types::effects::TransactionEffects;
use sui_types::storage::MarkerValue;
use tidehunter::key_shape::KeySpaceConfig;
use tidehunter::minibytes::Bytes;
use tidehunter::wal::WalPosition;
use typed_store::metrics::SamplingInterval;
use typed_store::rocks::util::{empty_compaction_filter, reference_count_merge_operator};
use typed_store::rocks::{
    default_db_options, read_size_from_env, DBBatch, DBMap, DBMapTableConfigMap, DBOptions,
    MetricConf,
};
use typed_store::traits::{Map, TableSummary, TypedStoreDebug};

use crate::authority::authority_store_types::{
    get_store_object_pair, try_construct_object, ObjectContentDigest, StoreData,
    StoreMoveObjectWrapper, StoreObject, StoreObjectPair, StoreObjectValue, StoreObjectWrapper,
};
use crate::authority::epoch_start_configuration::EpochStartConfiguration;
use typed_store::tidehunter::th_db_map::{key_shape_builder, open_thdb, ThDbBatch, ThDbMap};
use typed_store::DBMapUtils;

const ENV_VAR_OBJECTS_BLOCK_CACHE_SIZE: &str = "OBJECTS_BLOCK_CACHE_MB";
pub(crate) const ENV_VAR_LOCKS_BLOCK_CACHE_SIZE: &str = "LOCKS_BLOCK_CACHE_MB";
const ENV_VAR_TRANSACTIONS_BLOCK_CACHE_SIZE: &str = "TRANSACTIONS_BLOCK_CACHE_MB";
const ENV_VAR_EFFECTS_BLOCK_CACHE_SIZE: &str = "EFFECTS_BLOCK_CACHE_MB";
const ENV_VAR_EVENTS_BLOCK_CACHE_SIZE: &str = "EVENTS_BLOCK_CACHE_MB";
const ENV_VAR_INDIRECT_OBJECTS_BLOCK_CACHE_SIZE: &str = "INDIRECT_OBJECTS_BLOCK_CACHE_MB";

/// Options to apply to every column family of the `perpetual` DB.
#[derive(Default)]
pub struct AuthorityPerpetualTablesOptions {
    /// Whether to enable write stalling on all column families.
    pub enable_write_stall: bool,
}

impl AuthorityPerpetualTablesOptions {
    fn apply_to(&self, mut db_options: DBOptions) -> DBOptions {
        if !self.enable_write_stall {
            db_options = db_options.disable_write_throttling();
        }
        db_options
    }
}

/// AuthorityPerpetualTables contains data that must be preserved from one epoch to the next.
// #[derive(DBMapUtils)]
pub struct AuthorityPerpetualTables {
    /// This is a map between the object (ID, version) and the latest state of the object, namely the
    /// state that is needed to process new transactions.
    /// State is represented by `StoreObject` enum, which is either a move module, a move object, or
    /// a pointer to an object stored in the `indirect_move_objects` table.
    ///
    /// Note that while this map can store all versions of an object, we will eventually
    /// prune old object versions from the db.
    ///
    /// IMPORTANT: object versions must *only* be pruned if they appear as inputs in some
    /// TransactionEffects. Simply pruning all objects but the most recent is an error!
    /// This is because there can be partially executed transactions whose effects have not yet
    /// been written out, and which must be retried. But, they cannot be retried unless their input
    /// objects are still accessible!
    pub(crate) objects: ThDbMap<ObjectKey, StoreObjectWrapper>,

    pub(crate) indirect_move_objects: ThDbMap<ObjectContentDigest, StoreMoveObjectWrapper>,

    /// This is a map between object references of currently active objects that can be mutated.
    ///
    /// For old epochs, it may also contain the transaction that they are lock on for use by this
    /// specific validator. The transaction locks themselves are now in AuthorityPerEpochStore.
    pub(crate) live_owned_object_markers: ThDbMap<ObjectRef, Option<LockDetailsWrapperDeprecated>>,

    /// This is a map between the transaction digest and the corresponding transaction that's known to be
    /// executable. This means that it may have been executed locally, or it may have been synced through
    /// state-sync but hasn't been executed yet.
    pub(crate) transactions: ThDbMap<TransactionDigest, TrustedTransaction>,

    /// A map between the transaction digest of a certificate to the effects of its execution.
    /// We store effects into this table in two different cases:
    /// 1. When a transaction is synced through state_sync, we store the effects here. These effects
    ///     are known to be final in the network, but may not have been executed locally yet.
    /// 2. When the transaction is executed locally on this node, we store the effects here. This means that
    ///     it's possible to store the same effects twice (once for the synced transaction, and once for the executed).
    ///
    /// It's also possible for the effects to be reverted if the transaction didn't make it into the epoch.
    pub(crate) effects: ThDbMap<TransactionEffectsDigest, TransactionEffects>,

    /// Transactions that have been executed locally on this node. We need this table since the `effects` table
    /// doesn't say anything about the execution status of the transaction on this node. When we wait for transactions
    /// to be executed, we wait for them to appear in this table. When we revert transactions, we remove them from both
    /// tables.
    pub(crate) executed_effects: ThDbMap<TransactionDigest, TransactionEffectsDigest>,

    // Currently this is needed in the validator for returning events during process certificates.
    // We could potentially remove this if we decided not to provide events in the execution path.
    // TODO: Figure out what to do with this table in the long run.
    // Also we need a pruning policy for this table. We can prune this table along with tx/effects.
    pub(crate) events: ThDbMap<(TransactionEventsDigest, usize), Event>,

    /// DEPRECATED in favor of the table of the same name in authority_per_epoch_store.
    /// Please do not add new accessors/callsites.
    /// When transaction is executed via checkpoint executor, we store association here
    pub(crate) executed_transactions_to_checkpoint:
        ThDbMap<TransactionDigest, (EpochId, CheckpointSequenceNumber)>,

    // Finalized root state accumulator for epoch, to be included in CheckpointSummary
    // of last checkpoint of epoch. These values should only ever be written once
    // and never changed
    pub(crate) root_state_hash_by_epoch: ThDbMap<EpochId, (CheckpointSequenceNumber, Accumulator)>,

    /// Parameters of the system fixed at the epoch start
    pub(crate) epoch_start_configuration: ThDbMap<(), EpochStartConfiguration>,

    /// A singleton table that stores latest pruned checkpoint. Used to keep objects pruner progress
    pub pruned_checkpoint: ThDbMap<(), CheckpointSequenceNumber>,

    /// Expected total amount of SUI in the network. This is expected to remain constant
    /// throughout the lifetime of the network. We check it at the end of each epoch if
    /// expensive checks are enabled. We cannot use 10B today because in tests we often
    /// inject extra gas objects into genesis.
    pub(crate) expected_network_sui_amount: ThDbMap<(), u64>,

    /// Expected imbalance between storage fund balance and the sum of storage rebate of all live objects.
    /// This could be non-zero due to bugs in earlier protocol versions.
    /// This number is the result of storage_fund_balance - sum(storage_rebate).
    pub(crate) expected_storage_fund_imbalance: ThDbMap<(), i64>,

    /// Table that stores the set of received objects and deleted objects and the version at
    /// which they were received. This is used to prevent possible race conditions around receiving
    /// objects (since they are not locked by the transaction manager) and for tracking shared
    /// objects that have been deleted. This table is meant to be pruned per-epoch, and all
    /// previous epochs other than the current epoch may be pruned safely.
    pub(crate) object_per_epoch_marker_table: ThDbMap<(EpochId, ObjectKey), MarkerValue>,
}

impl AuthorityPerpetualTables {
    pub fn path(parent_path: &Path) -> PathBuf {
        parent_path.join("perpetual")
    }

    pub fn open(
        parent_path: &Path,
        db_options_override: Option<AuthorityPerpetualTablesOptions>,
    ) -> Self {
        Self::open_with_registry(parent_path, db_options_override, &Registry::default())
    }

    pub fn open_with_registry(
        parent_path: &Path,
        db_options_override: Option<AuthorityPerpetualTablesOptions>,
        registry: &Registry,
    ) -> Self {
        let db_options_override = db_options_override.unwrap_or_default();
        let db_options =
            db_options_override.apply_to(default_db_options().optimize_db_for_write_throughput(4));
        let table_options = DBMapTableConfigMap::new(BTreeMap::from([
            (
                "objects".to_string(),
                objects_table_config(db_options.clone()),
            ),
            (
                "indirect_move_objects".to_string(),
                indirect_move_objects_table_config(db_options.clone()),
            ),
            (
                "owned_object_transaction_locks".to_string(),
                owned_object_transaction_locks_table_config(db_options.clone()),
            ),
            (
                "transactions".to_string(),
                transactions_table_config(db_options.clone()),
            ),
            (
                "effects".to_string(),
                effects_table_config(db_options.clone()),
            ),
            (
                "events".to_string(),
                events_table_config(db_options.clone()),
            ),
        ]));
        let path = Self::path(parent_path);

        // todo - update those values when adding new space
        let const_spaces = 6;
        let frac_spaces = 8;
        let mut builder = key_shape_builder(const_spaces, frac_spaces);
        // 6 const key spaces
        let root_state_hash_by_epoch = builder.const_key_space("root_state_hash_by_epoch", 8, 1);
        let epoch_start_configuration = builder.const_key_space("epoch_start_configuration", 0, 1);
        let pruned_checkpoint = builder.const_key_space("pruned_checkpoint", 0, 1);
        let expected_network_sui_amount = builder.const_key_space("expected_network_sui_amount", 0, 1);
        let expected_storage_fund_imbalance =
            builder.const_key_space("expected_storage_fund_imbalance", 0, 1);
        // todo chop off too?
        // decide what to do with it - make it frac_key_space or get rid of?
        let indirect_move_objects = builder.const_key_space("indirect_move_objects", 32, 1);
        // todo - this is very confusing - need to fix
        let const_spaces_round_up = builder.pad_const_space();

        // 8 frac key spaces
        let objects_config = KeySpaceConfig::new().with_compactor(Box::new(objects_compactor));
        let objects = builder.frac_key_space_config("objects", 32 + 8, 1, objects_config);
        let live_owned_object_markers = builder.frac_key_space("live_owned_object_markers", 32 + 8 + 32 + 8, 1);
        let transactions = builder.frac_key_space("transactions", 32, 1);
        let effects = builder.frac_key_space("effects", 32, 1);
        let executed_effects = builder.frac_key_space("executed_effects",32, 1);
        let events = builder.frac_key_space("events", 32 + 8, 1);
        let executed_transactions_to_checkpoint =
            builder.frac_key_space("executed_transactions_to_checkpoint", 32, 1);

        // key_offset is set to 8 to hash by object id rather then epoch
        let object_per_epoch_marker_table = builder.frac_key_space_config(
            "object_per_epoch_marker_table",
            32 + 8 + 8,
            1,
            KeySpaceConfig::new_with_key_offset(8),
        );

        let key_shape = builder.build();
        let thdb = open_thdb(&path, key_shape, const_spaces_round_up, registry);
        // Effect digest, transaction digest and other keys derived from Digest are prefixed with
        // [0, 0, ..., 32], which we can remove to reduce the size of thdb index.
        // Smaller index means less write amplification, and
        // the size of the prefix is 25% of the useful key length, so not negligible.
        let mut digest_prefix = vec![0; 8];
        digest_prefix[7] = 32;
        Self {
            objects: ThDbMap::new(&thdb, objects),
            indirect_move_objects: ThDbMap::new(&thdb, indirect_move_objects),
            live_owned_object_markers: ThDbMap::new(&thdb, live_owned_object_markers),
            transactions: ThDbMap::new_with_rm_prefix(&thdb, transactions, digest_prefix.clone()),
            effects: ThDbMap::new_with_rm_prefix(&thdb, effects, digest_prefix.clone()),
            executed_effects: ThDbMap::new_with_rm_prefix(
                &thdb,
                executed_effects,
                digest_prefix.clone(),
            ),
            events: ThDbMap::new_with_rm_prefix(&thdb, events, digest_prefix.clone()),
            executed_transactions_to_checkpoint: ThDbMap::new_with_rm_prefix(
                &thdb,
                executed_transactions_to_checkpoint,
                digest_prefix.clone(),
            ),
            root_state_hash_by_epoch: ThDbMap::new(&thdb, root_state_hash_by_epoch),
            epoch_start_configuration: ThDbMap::new(&thdb, epoch_start_configuration),
            pruned_checkpoint: ThDbMap::new(&thdb, pruned_checkpoint),
            expected_network_sui_amount: ThDbMap::new(&thdb, expected_network_sui_amount),
            expected_storage_fund_imbalance: ThDbMap::new(&thdb, expected_storage_fund_imbalance),
            object_per_epoch_marker_table: ThDbMap::new(&thdb, object_per_epoch_marker_table),
        }
    }

    pub fn open_readonly(parent_path: &Path) -> AuthorityPerpetualTables {
        unimplemented!()
    }

    // This is used by indexer to find the correct version of dynamic field child object.
    // We do not store the version of the child object, but because of lamport timestamp,
    // we know the child must have version number less then or eq to the parent.
    pub fn find_object_lt_or_eq_version(
        &self,
        object_id: ObjectID,
        version: SequenceNumber,
    ) -> SuiResult<Option<Object>> {
        let last = self.objects.last_in_range(
            &ObjectKey::min_for_id(&object_id),
            &ObjectKey(object_id, version),
        );
        match last {
            Some((key, o)) => self.object(&key, o),
            None => Ok(None),
        }
    }

    fn construct_object(
        &self,
        object_key: &ObjectKey,
        store_object: StoreObjectValue,
    ) -> Result<Object, SuiError> {
        let indirect_object = match store_object.data {
            StoreData::IndirectObject(ref metadata) => self
                .indirect_move_objects
                .get(&metadata.digest)?
                .map(|o| o.migrate().into_inner()),
            _ => None,
        };
        try_construct_object(object_key, store_object, indirect_object)
    }

    // Constructs `sui_types::object::Object` from `StoreObjectWrapper`.
    // Returns `None` if object was deleted/wrapped
    pub fn object(
        &self,
        object_key: &ObjectKey,
        store_object: StoreObjectWrapper,
    ) -> Result<Option<Object>, SuiError> {
        let StoreObject::Value(store_object) = store_object.migrate().into_inner() else {
            return Ok(None);
        };
        Ok(Some(self.construct_object(object_key, store_object)?))
    }

    pub fn object_reference(
        &self,
        object_key: &ObjectKey,
        store_object: StoreObjectWrapper,
    ) -> Result<ObjectRef, SuiError> {
        let obj_ref = match store_object.migrate().into_inner() {
            StoreObject::Value(object) => self
                .construct_object(object_key, object)?
                .compute_object_reference(),
            StoreObject::Deleted => (
                object_key.0,
                object_key.1,
                ObjectDigest::OBJECT_DIGEST_DELETED,
            ),
            StoreObject::Wrapped => (
                object_key.0,
                object_key.1,
                ObjectDigest::OBJECT_DIGEST_WRAPPED,
            ),
        };
        Ok(obj_ref)
    }

    pub fn tombstone_reference(
        &self,
        object_key: &ObjectKey,
        store_object: &StoreObjectWrapper,
    ) -> Result<Option<ObjectRef>, SuiError> {
        let obj_ref = match store_object.inner() {
            StoreObject::Deleted => Some((
                object_key.0,
                object_key.1,
                ObjectDigest::OBJECT_DIGEST_DELETED,
            )),
            StoreObject::Wrapped => Some((
                object_key.0,
                object_key.1,
                ObjectDigest::OBJECT_DIGEST_WRAPPED,
            )),
            _ => None,
        };
        Ok(obj_ref)
    }

    pub fn get_latest_object_ref_or_tombstone(
        &self,
        object_id: ObjectID,
    ) -> Result<Option<ObjectRef>, SuiError> {
        let last = self.objects.last_in_range(
            &ObjectKey::min_for_id(&object_id),
            &ObjectKey::max_for_id(&object_id),
        );

        if let Some((object_key, value)) = last {
            debug_assert_eq!(object_key.0, object_id);
            return Ok(Some(self.object_reference(&object_key, value)?));
        }
        Ok(None)
    }

    pub fn get_latest_object_or_tombstone(
        &self,
        object_id: ObjectID,
    ) -> Result<Option<(ObjectKey, StoreObjectWrapper)>, SuiError> {
        let last = self.objects.last_in_range(
            &ObjectKey::min_for_id(&object_id),
            &ObjectKey::max_for_id(&object_id),
        );

        if let Some((object_key, value)) = last {
            debug_assert_eq!(object_key.0, object_id);
            return Ok(Some((object_key, value)));
        }
        Ok(None)
    }

    pub fn get_recovery_epoch_at_restart(&self) -> SuiResult<EpochId> {
        Ok(self
            .epoch_start_configuration
            .get(&())?
            .expect("Must have current epoch.")
            .epoch_start_state()
            .epoch())
    }

    pub fn set_epoch_start_configuration(
        &self,
        epoch_start_configuration: &EpochStartConfiguration,
    ) -> SuiResult {
        let mut wb = self.epoch_start_configuration.batch();
        wb.insert_batch(
            &self.epoch_start_configuration,
            std::iter::once(((), epoch_start_configuration)),
        )?;
        wb.write()?;
        Ok(())
    }

    pub fn get_highest_pruned_checkpoint(&self) -> SuiResult<CheckpointSequenceNumber> {
        Ok(self.pruned_checkpoint.get(&())?.unwrap_or_default())
    }

    pub fn set_highest_pruned_checkpoint(
        &self,
        wb: &mut ThDbBatch,
        checkpoint_number: CheckpointSequenceNumber,
    ) -> SuiResult {
        wb.insert_batch(&self.pruned_checkpoint, [((), checkpoint_number)])?;
        Ok(())
    }

    pub fn get_transaction(
        &self,
        digest: &TransactionDigest,
    ) -> SuiResult<Option<TrustedTransaction>> {
        let Some(transaction) = self.transactions.get(digest)? else {
            return Ok(None);
        };
        Ok(Some(transaction))
    }

    pub fn get_effects(&self, digest: &TransactionDigest) -> SuiResult<Option<TransactionEffects>> {
        let Some(effect_digest) = self.executed_effects.get(digest)? else {
            return Ok(None);
        };
        Ok(self.effects.get(&effect_digest)?)
    }

    // DEPRECATED as the backing table has been moved to authority_per_epoch_store.
    // Please do not add new accessors/callsites.
    pub fn get_checkpoint_sequence_number(
        &self,
        digest: &TransactionDigest,
    ) -> SuiResult<Option<(EpochId, CheckpointSequenceNumber)>> {
        Ok(self.executed_transactions_to_checkpoint.get(digest)?)
    }

    pub fn get_newer_object_keys(
        &self,
        object: &(ObjectID, SequenceNumber),
    ) -> SuiResult<Vec<ObjectKey>> {
        let mut objects = vec![];
        for result in self.objects.safe_iter_with_bounds(
            Some(ObjectKey(object.0, object.1.next())),
            Some(ObjectKey(object.0, VersionNumber::MAX)),
        ) {
            let (key, _) = result?;
            objects.push(key);
        }
        Ok(objects)
    }

    pub fn set_highest_pruned_checkpoint_without_wb(
        &self,
        checkpoint_number: CheckpointSequenceNumber,
    ) -> SuiResult {
        let mut wb = self.pruned_checkpoint.batch();
        self.set_highest_pruned_checkpoint(&mut wb, checkpoint_number)?;
        wb.write()?;
        Ok(())
    }

    pub fn database_is_empty(&self) -> SuiResult<bool> {
        Ok(self.objects.unbounded_iter().next().is_none())
    }

    pub fn iter_live_object_set(&self, include_wrapped_object: bool) -> LiveSetIter<'_> {
        LiveSetIter {
            iter: self.objects.unbounded_iter(),
            tables: self,
            prev: None,
            include_wrapped_object,
        }
    }

    pub fn range_iter_live_object_set(
        &self,
        // lower_bound: Option<ObjectID>,
        // upper_bound: Option<ObjectID>,
        include_wrapped_object: bool,
    ) -> LiveSetIter<'_> {
        unimplemented!("range_iter_live_object_set"); // todo
        // let lower_bound = lower_bound.as_ref().map(ObjectKey::min_for_id);
        // let upper_bound = upper_bound.as_ref().map(ObjectKey::max_for_id);
        //
        // LiveSetIter {
        //     iter: self.objects.iter_with_bounds(lower_bound, upper_bound),
        //     tables: self,
        //     prev: None,
        //     include_wrapped_object,
        // }
    }

    pub fn checkpoint_db(&self, path: &Path) -> SuiResult {
        // This checkpoints the entire db and not just objects table
        unimplemented!("checkpoint_db")
        // self.objects.checkpoint_db(path).map_err(Into::into)
    }

    pub fn reset_db_for_execution_since_genesis(&self) -> SuiResult {
        unimplemented!()
        // // TODO: Add new tables that get added to the db automatically
        // self.objects.unsafe_clear()?;
        // self.indirect_move_objects.unsafe_clear()?;
        // self.live_owned_object_markers.unsafe_clear()?;
        // self.executed_effects.unsafe_clear()?;
        // self.events.unsafe_clear()?;
        // self.executed_transactions_to_checkpoint.unsafe_clear()?;
        // self.root_state_hash_by_epoch.unsafe_clear()?;
        // self.epoch_start_configuration.unsafe_clear()?;
        // self.pruned_checkpoint.unsafe_clear()?;
        // self.expected_network_sui_amount.unsafe_clear()?;
        // self.expected_storage_fund_imbalance.unsafe_clear()?;
        // self.object_per_epoch_marker_table.unsafe_clear()?;
        // self.objects.rocksdb.flush()?;
        // Ok(())
    }

    pub fn get_root_state_hash(
        &self,
        epoch: EpochId,
    ) -> SuiResult<Option<(CheckpointSequenceNumber, Accumulator)>> {
        Ok(self.root_state_hash_by_epoch.get(&epoch)?)
    }

    pub fn insert_root_state_hash(
        &self,
        epoch: EpochId,
        last_checkpoint_of_epoch: CheckpointSequenceNumber,
        accumulator: Accumulator,
    ) -> SuiResult {
        self.root_state_hash_by_epoch
            .insert(&epoch, &(last_checkpoint_of_epoch, accumulator))?;
        Ok(())
    }

    pub fn insert_object_test_only(&self, object: Object) -> SuiResult {
        let object_reference = object.compute_object_reference();
        let StoreObjectPair(wrapper, _indirect_object) = get_store_object_pair(object, usize::MAX);
        let mut wb = self.objects.batch();
        wb.insert_batch(
            &self.objects,
            std::iter::once((ObjectKey::from(object_reference), wrapper)),
        )?;
        wb.write()?;
        Ok(())
    }

    /// Read an object and return it, or Ok(None) if the object was not found.
    pub fn get_object_fallible(
        &self,
        object_id: &ObjectID,
    ) -> SuiResult<Option<Object>> {
        let obj_entry = self.objects.last_in_range(
            &ObjectKey::min_for_id(object_id),
            &ObjectKey::max_for_id(object_id),
        );

        match obj_entry {
            Some((ObjectKey(obj_id, version), obj)) if obj_id == *object_id => {
                Ok(self.object(&ObjectKey(obj_id, version), obj)?)
            }
            _ => Ok(None),
        }
    }

    pub fn get_object_by_key_fallible(
        &self,
        object_id: &ObjectID,
        version: VersionNumber,
    ) -> SuiResult<Option<Object>> {
        Ok(self
            .objects
            .get(&ObjectKey(*object_id, version))?
            .and_then(|object| {
                self.object(&ObjectKey(*object_id, version), object)
                    .expect("object construction error")
            }))
    }
}

impl ObjectStore for AuthorityPerpetualTables {
    /// Read an object and return it, or Ok(None) if the object was not found.
    fn get_object(&self, object_id: &ObjectID) -> Option<Object> {
        self.get_object_fallible(object_id).expect("db error")
    }

    fn get_object_by_key(&self, object_id: &ObjectID, version: VersionNumber) -> Option<Object> {
        self.get_object_by_key_fallible(object_id, version)
            .expect("db error")
    }
}

pub struct LiveSetIter<'a> {
    iter:
        <ThDbMap<ObjectKey, StoreObjectWrapper> as Map<'a, ObjectKey, StoreObjectWrapper>>::Iterator,
    tables: &'a AuthorityPerpetualTables,
    prev: Option<(ObjectKey, StoreObjectWrapper)>,
    /// Whether a wrapped object is considered as a live object.
    include_wrapped_object: bool,
}

#[derive(Eq, PartialEq, Debug, Clone, Deserialize, Serialize, Hash)]
pub enum LiveObject {
    Normal(Object),
    Wrapped(ObjectKey),
}

impl LiveObject {
    pub fn object_id(&self) -> ObjectID {
        match self {
            LiveObject::Normal(obj) => obj.id(),
            LiveObject::Wrapped(key) => key.0,
        }
    }

    pub fn version(&self) -> SequenceNumber {
        match self {
            LiveObject::Normal(obj) => obj.version(),
            LiveObject::Wrapped(key) => key.1,
        }
    }

    pub fn object_reference(&self) -> ObjectRef {
        match self {
            LiveObject::Normal(obj) => obj.compute_object_reference(),
            LiveObject::Wrapped(key) => (key.0, key.1, ObjectDigest::OBJECT_DIGEST_WRAPPED),
        }
    }

    pub fn to_normal(self) -> Option<Object> {
        match self {
            LiveObject::Normal(object) => Some(object),
            LiveObject::Wrapped(_) => None,
        }
    }
}

impl LiveSetIter<'_> {
    fn store_object_wrapper_to_live_object(
        &self,
        object_key: ObjectKey,
        store_object: StoreObjectWrapper,
    ) -> Option<LiveObject> {
        match store_object.migrate().into_inner() {
            StoreObject::Value(object) => {
                let object = self
                    .tables
                    .construct_object(&object_key, object)
                    .expect("Constructing object from store cannot fail");
                Some(LiveObject::Normal(object))
            }
            StoreObject::Wrapped => {
                if self.include_wrapped_object {
                    Some(LiveObject::Wrapped(object_key))
                } else {
                    None
                }
            }
            StoreObject::Deleted => None,
        }
    }
}

impl Iterator for LiveSetIter<'_> {
    type Item = LiveObject;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((next_key, next_value)) = self.iter.next() {
                let prev = self.prev.take();
                self.prev = Some((next_key, next_value));

                if let Some((prev_key, prev_value)) = prev {
                    if prev_key.0 != next_key.0 {
                        let live_object =
                            self.store_object_wrapper_to_live_object(prev_key, prev_value);
                        if live_object.is_some() {
                            return live_object;
                        }
                    }
                }
                continue;
            }
            if let Some((key, value)) = self.prev.take() {
                let live_object = self.store_object_wrapper_to_live_object(key, value);
                if live_object.is_some() {
                    return live_object;
                }
            }
            return None;
        }
    }
}

// These functions are used to initialize the DB tables
fn owned_object_transaction_locks_table_config(db_options: DBOptions) -> DBOptions {
    DBOptions {
        options: db_options
            .clone()
            .optimize_for_write_throughput()
            .optimize_for_read(read_size_from_env(ENV_VAR_LOCKS_BLOCK_CACHE_SIZE).unwrap_or(1024))
            .options,
        rw_options: db_options.rw_options.set_ignore_range_deletions(false),
    }
}

fn objects_table_config(db_options: DBOptions) -> DBOptions {
    db_options
        .optimize_for_write_throughput()
        .optimize_for_read(read_size_from_env(ENV_VAR_OBJECTS_BLOCK_CACHE_SIZE).unwrap_or(5 * 1024))
}

fn transactions_table_config(db_options: DBOptions) -> DBOptions {
    db_options
        .optimize_for_write_throughput()
        .optimize_for_point_lookup(
            read_size_from_env(ENV_VAR_TRANSACTIONS_BLOCK_CACHE_SIZE).unwrap_or(512),
        )
}

fn effects_table_config(db_options: DBOptions) -> DBOptions {
    db_options
        .optimize_for_write_throughput()
        .optimize_for_point_lookup(
            read_size_from_env(ENV_VAR_EFFECTS_BLOCK_CACHE_SIZE).unwrap_or(1024),
        )
}

fn events_table_config(db_options: DBOptions) -> DBOptions {
    db_options
        .optimize_for_write_throughput()
        .optimize_for_read(read_size_from_env(ENV_VAR_EVENTS_BLOCK_CACHE_SIZE).unwrap_or(1024))
}

fn indirect_move_objects_table_config(mut db_options: DBOptions) -> DBOptions {
    db_options = db_options
        .optimize_for_write_throughput()
        .optimize_for_point_lookup(
            read_size_from_env(ENV_VAR_INDIRECT_OBJECTS_BLOCK_CACHE_SIZE).unwrap_or(512),
        );
    db_options.options.set_merge_operator(
        "refcount operator",
        reference_count_merge_operator,
        reference_count_merge_operator,
    );
    db_options
        .options
        .set_compaction_filter("empty filter", empty_compaction_filter);
    db_options
}

fn objects_compactor(index: &mut BTreeMap<Bytes, WalPosition>) {
    let mut retain = HashSet::new();
    let mut previous: Option<&[u8]> = None;
    const OID_SIZE: usize = 32;
    for (key, _) in index.iter().rev() {
        if let Some(previous) = previous {
            if previous == &key[..OID_SIZE] {
                continue;
            }
        }
        previous = Some(&key[..OID_SIZE]);
        retain.insert(key.clone());
    }
    index.retain(|k, _| retain.contains(k));
}

#[cfg(test)]
mod tests {
    use typed_store::rocks::be_fix_int_ser;
    use super::*;

    #[test]
    fn test_object_compactor() {
        let o1 = ObjectID::new([1u8; 32]);
        let k11 = ObjectKey(o1, SequenceNumber::from_u64(1));
        let k12 = ObjectKey(o1, SequenceNumber::from_u64(5));
        let k13 = ObjectKey(o1, SequenceNumber::from_u64(8));
        let o2 = ObjectID::new([2u8; 32]);
        let k22 = ObjectKey(o2, SequenceNumber::from_u64(3));
        let mut data: BTreeMap<_, _> = [k11, k12, k13, k22].into_iter().map(|k| {
            (Bytes::from(be_fix_int_ser(&k).unwrap()), WalPosition::INVALID)
        }).collect();
        objects_compactor(&mut data);
        assert_eq!(data.len(), 2);
        assert!(data.contains_key(&Bytes::from(be_fix_int_ser(&k13).unwrap())));
        assert!(data.contains_key(&Bytes::from(be_fix_int_ser(&k22).unwrap())));
    }
}