// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use rand::rngs::StdRng;
use rand::SeedableRng;
use simulacrum::Simulacrum;
use std::{path::PathBuf, time::Duration};
use sui_indexer_alt::{
    config::{IndexerConfig, Merge},
    start_indexer,
};
use sui_indexer_alt_framework::{
    ingestion::ClientArgs, models::cp_sequence_numbers::tx_interval, IndexerArgs,
};
use sui_indexer_alt_schema::schema::{kv_checkpoints, kv_epoch_starts};
use sui_pg_db::{
    temp::{get_available_port, TempDb},
    Db, DbArgs,
};
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
// test for a pruner that doesn't need interval lookups

/// Prepares a test indexer configuration, deferring to the default config for top-level fields, but
/// explicitly setting all fields of the `cp_sequence_numbers` pipeline layer. This can then be
/// passed to the `indexer_config` arg of `start_indexer`.
#[cfg(test)]
fn load_indexer_config(path_str: &str) -> IndexerConfig {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push(format!("tests/pruning/configs/{}", path_str));
    let config_str = std::fs::read_to_string(&path)
        .expect(&format!("Failed to read test config file at {:?}", path));
    toml::from_str(&config_str).expect("Failed to parse test config TOML")
}

#[cfg(test)]
async fn setup_temp_resources() -> (TempDb, TempDir) {
    let temp_db = TempDb::new().unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    (temp_db, temp_dir)
}

#[cfg(test)]
async fn setup_test_env(
    db_url: String,
    data_ingestion_path: PathBuf,
    indexer_config: IndexerConfig,
) -> (
    Simulacrum<StdRng>,
    Db,
    JoinHandle<anyhow::Result<()>>,
    CancellationToken,
) {
    // Set up simulacrum
    let rng = StdRng::from_seed([12; 32]);
    let mut sim = Simulacrum::new_with_rng(rng);
    sim.set_data_ingestion_path(data_ingestion_path.clone());

    // Set up direct db pool for test assertions
    let db = Db::for_write(DbArgs {
        database_url: db_url.parse().unwrap(),
        db_connection_pool_size: 1,
        connection_timeout_ms: 60_000,
    })
    .await
    .unwrap();

    // Set up indexer
    let db_args = DbArgs {
        database_url: db_url.parse().unwrap(),
        db_connection_pool_size: 10,
        connection_timeout_ms: 60_000,
    };

    let prom_address = format!("127.0.0.1:{}", get_available_port())
        .parse()
        .unwrap();
    let indexer_args = IndexerArgs {
        metrics_address: prom_address,
        ..Default::default()
    };

    let client_args = ClientArgs {
        remote_store_url: None,
        local_ingestion_path: Some(data_ingestion_path),
    };

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();

    // Spawn the indexer in a separate task
    let indexer_handle = tokio::spawn(async move {
        start_indexer(
            db_args,
            indexer_args,
            client_args,
            indexer_config,
            true,
            Some(cancel_clone),
        )
        .await
    });

    (sim, db, indexer_handle, cancel)
}

#[tokio::test]
pub async fn test_cp_sequence_numbers() -> () {
    let indexer_config = load_indexer_config("base_config.toml");
    let cp_sequence_numbers_config = load_indexer_config("cp_sequence_numbers.toml");
    let merged_config = indexer_config.merge(cp_sequence_numbers_config);
    let (temp_db, temp_dir) = setup_temp_resources().await;
    let db_url = temp_db.database().url().as_str().to_owned();
    let data_ingestion_path = temp_dir.path().to_path_buf();

    let (mut sim, db, indexer_handle, cancel) =
        setup_test_env(db_url, data_ingestion_path, merged_config).await;

    // Do your test assertions here
    // ...
    sim.create_checkpoint();
    sim.create_checkpoint();

    let mut conn = db
        .connect()
        .await
        .expect("Failed to retrieve DB connection");

    let timeout_duration = Duration::from_secs(5); // Adjust timeout as needed
    loop {
        match timeout(timeout_duration, tx_interval(&mut conn, 0..2)).await {
            // Timeout occurred
            Err(_elapsed) => {
                // If we hit timeout, return early from the test
                cancel.cancel();
                return;
            }
            // Got a result within timeout
            Ok(result) => match result {
                // Got valid range, break loop
                Ok(_) => break,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
        }
    }

    cancel.cancel();

    // Wait for the indexer to shut down
    let _ = indexer_handle.await.expect("Indexer task panicked");
}

#[tokio::test]
pub async fn test_kv_epoch_starts_cross_epoch() -> () {
    let merged_config = load_indexer_config("base_config.toml")
        .merge(load_indexer_config("cp_sequence_numbers.toml"))
        .merge(load_indexer_config("kv_epoch_starts_diff_epoch.toml"));
    let (temp_db, temp_dir) = setup_temp_resources().await;
    let db_url = temp_db.database().url().as_str().to_owned();
    let data_ingestion_path = temp_dir.path().to_path_buf();

    let (mut sim, db, indexer_handle, cancel) =
        setup_test_env(db_url, data_ingestion_path, merged_config).await;

    sim.advance_epoch(true);
    sim.advance_epoch(true);
    sim.advance_epoch(true);

    let mut conn = db
        .connect()
        .await
        .expect("Failed to retrieve DB connection");

    let timeout_duration = Duration::from_secs(5); // Adjust timeout as needed
    loop {
        match timeout(timeout_duration, tx_interval(&mut conn, 0..3)).await {
            // Timeout occurred
            Err(_elapsed) => {
                // If we hit timeout, return early from the test
                cancel.cancel();
                return;
            }
            // Got a result within timeout
            Ok(result) => match result {
                // Got valid range, break loop
                Ok(_) => break,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
        }
    }

    use diesel::query_dsl::select_dsl::SelectDsl;
    use diesel_async::RunQueryDsl;

    let timeout_duration = Duration::from_secs(5);
    tokio::select! {
        _ = tokio::time::sleep(timeout_duration) => {
            cancel.cancel();
            return;
        }
        _ = async {
            loop {
                match kv_epoch_starts::table
                    .select(kv_epoch_starts::epoch)
                    .load::<i64>(&mut conn)
                    .await
                {
                    Ok(epochs) if epochs == vec![3] => break,
                    Ok(_) | Err(_) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        } => {}
    }

    cancel.cancel();

    // Wait for the indexer to shut down
    let _ = indexer_handle.await.expect("Indexer task panicked");
}

#[tokio::test]
pub async fn test_kv_epoch_starts_same_epoch() -> () {
    let merged_config = load_indexer_config("base_config.toml")
        .merge(load_indexer_config("cp_sequence_numbers.toml"))
        .merge(load_indexer_config("kv_epoch_starts_diff_epoch.toml"));
    let (temp_db, temp_dir) = setup_temp_resources().await;
    let db_url = temp_db.database().url().as_str().to_owned();
    let data_ingestion_path = temp_dir.path().to_path_buf();

    let (mut sim, db, indexer_handle, cancel) =
        setup_test_env(db_url, data_ingestion_path, merged_config).await;

    sim.advance_epoch(true);
    sim.create_checkpoint();
    sim.create_checkpoint();
    sim.create_checkpoint();

    let mut conn = db
        .connect()
        .await
        .expect("Failed to retrieve DB connection");

    let timeout_duration = Duration::from_secs(5); // Adjust timeout as needed
    loop {
        match timeout(timeout_duration, tx_interval(&mut conn, 0..4)).await {
            // Timeout occurred
            Err(_elapsed) => {
                // If we hit timeout, return early from the test
                cancel.cancel();
                return;
            }
            // Got a result within timeout
            Ok(result) => match result {
                // Got valid range, break loop
                Ok(_) => break,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
        }
    }

    use diesel::query_dsl::select_dsl::SelectDsl;
    use diesel_async::RunQueryDsl;

    let timeout_duration = Duration::from_secs(5);
    tokio::select! {
        _ = tokio::time::sleep(timeout_duration) => {
            cancel.cancel();
            return;
        }
        _ = async {
            loop {
                match kv_epoch_starts::table
                    .select(kv_epoch_starts::epoch)
                    .load::<i64>(&mut conn)
                    .await
                {
                    Ok(epochs) if epochs == vec![1] => break,
                    Ok(_) | Err(_) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        } => {}
    }

    cancel.cancel();

    // Wait for the indexer to shut down
    let _ = indexer_handle.await.expect("Indexer task panicked");
}

#[tokio::test]
pub async fn test_kv_checkpoints_no_mapping() -> () {
    let merged_config =
        load_indexer_config("base_config.toml").merge(load_indexer_config("kv_checkpoints.toml"));
    let (temp_db, temp_dir) = setup_temp_resources().await;
    let db_url = temp_db.database().url().as_str().to_owned();
    let data_ingestion_path = temp_dir.path().to_path_buf();

    let (mut sim, db, indexer_handle, cancel) =
        setup_test_env(db_url, data_ingestion_path, merged_config).await;

    sim.create_checkpoint();
    sim.create_checkpoint();
    sim.create_checkpoint();

    let mut conn = db
        .connect()
        .await
        .expect("Failed to retrieve DB connection");

    use diesel::query_dsl::select_dsl::SelectDsl;
    use diesel_async::RunQueryDsl;

    let timeout_duration = Duration::from_secs(5);
    tokio::select! {
        _ = tokio::time::sleep(timeout_duration) => {
            cancel.cancel();
            return;
        }
        _ = async {
            loop {
                match kv_checkpoints::table
                    .select(kv_checkpoints::sequence_number)
                    .load::<i64>(&mut conn)
                    .await
                {
                    Ok(checkpoints) if checkpoints == vec![3] => break,
                    Ok(_) | Err(_) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        } => {}
    }

    cancel.cancel();

    // Wait for the indexer to shut down
    let _ = indexer_handle.await.expect("Indexer task panicked");
}
