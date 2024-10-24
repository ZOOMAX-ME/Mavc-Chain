// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, sync::Arc};

use mysten_metrics::spawn_monitored_task;
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{interval, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    handlers::Handler,
    metrics::IndexerMetrics,
    pipeline::{Batched, Indexed, PipelineConfig},
};

/// The collector task is responsible for gathering rows into batches which it then sends to a
/// committer task to write to the database. The task publishes batches in the following
/// circumstances:
///
/// - If `H::BATCH_SIZE` rows are pending, it will immediately schedule a batch to be gathered.
///
/// - If after sending one batch there is more data to be sent, it will immediately schedule the
///   next batch to be gathered (Each batch will contain at most `H::CHUNK_SIZ` rows).
///
/// - Otherwise, it will check for any data to write out at a regular interval (controlled by
///   `config.collect_interval`).
///
/// This task will shutdown if canceled via the `cancel` token, or if any of its channels are
/// closed.
pub(super) fn collector<H: Handler + 'static>(
    config: PipelineConfig,
    mut rx: mpsc::Receiver<Indexed<H>>,
    tx: mpsc::Sender<Batched<H>>,
    metrics: Arc<IndexerMetrics>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    spawn_monitored_task!(async move {
        // The `poll` interval controls the maximum time to wait between collecting batches,
        // regardless of number of rows pending.
        let mut poll = interval(config.collect_interval);
        poll.set_missed_tick_behavior(MissedTickBehavior::Delay);

        // Data for checkpoints that haven't been written yet.
        let mut pending: BTreeMap<u64, Indexed<H>> = BTreeMap::new();
        let mut pending_rows = 0;

        info!(pipeline = H::NAME, "Starting collector");

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!(pipeline = H::NAME, "Shutdown received, stopping collector");
                    break;
                }

                // Time to create another batch and push it to the committer.
                _ = poll.tick() => {
                    let guard = metrics
                        .collector_gather_latency
                        .with_label_values(&[H::NAME])
                        .start_timer();

                    let mut batch = Batched::new();
                    while !batch.is_full() {
                        let Some(mut entry) = pending.first_entry() else {
                            break;
                        };

                        let indexed = entry.get_mut();
                        indexed.batch_into(&mut batch);
                        if indexed.is_empty() {
                            entry.remove();
                        }
                    }

                    pending_rows -= batch.len();
                    let elapsed = guard.stop_and_record();
                    debug!(
                        pipeline = H::NAME,
                        elapsed_ms = elapsed * 1000.0,
                        rows = batch.len(),
                        pending = pending_rows,
                        "Gathered batch",
                    );

                    metrics
                        .total_collector_batches_created
                        .with_label_values(&[H::NAME])
                        .inc();

                    metrics
                        .collector_batch_size
                        .with_label_values(&[H::NAME])
                        .observe(batch.len() as f64);

                    if tx.send(batch).await.is_err() {
                        info!(pipeline = H::NAME, "Committer closed channel, stopping collector");
                        break;
                    }

                    if pending_rows > 0 {
                        poll.reset_immediately();
                    } else if rx.is_closed() {
                        info!(
                            pipeline = H::NAME,
                            "Processor closed channel, pending rows empty, stopping collector",
                        );
                        break;
                    }
                }

                Some(indexed) = rx.recv(), if pending_rows < H::MAX_PENDING_SIZE => {
                    metrics
                        .total_collector_rows_received
                        .with_label_values(&[H::NAME])
                        .inc_by(indexed.values.len() as u64);

                    pending_rows += indexed.values.len();
                    pending.insert(indexed.checkpoint(), indexed);

                    if pending_rows >= H::BATCH_SIZE {
                        poll.reset_immediately()
                    }
                }
            }
        }
    })
}