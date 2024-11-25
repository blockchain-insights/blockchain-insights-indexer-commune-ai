use std::collections::HashMap;
use std::sync::Arc;
use anyhow::{Context, Result};
use std::env;
use flecs_ecs::prelude::flecs::pipeline::OnUpdate;
use flecs_ecs::prelude::*;
use serde::{Deserialize, Serialize};
use dotenv::dotenv;
use neo4rs::{BoltInteger, BoltList, BoltMap, BoltType, Error, Graph, Node, ConfigBuilder};
use reqwest;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use tracing::{debug, error, info, span, warn, Level};

async fn initialize_indices(graph: &Graph, db_type: &str) -> Result<(), ProcessingError> {
    match db_type {
        "neo4j" => initialize_neo4j_indices(graph).await,
        "memgraph" => initialize_memgraph_indices(graph).await,
        _ => Err(ProcessingError::ProcessError("Unsupported database type".to_string()))
    }
}

async fn initialize_memgraph_indices(graph: &Graph) -> Result<(), ProcessingError> {
    let check_indices = "SHOW INDEX INFO;";
    let mut result = graph.execute(check_indices.into()).await?;
    let mut existing_indices = Vec::new();

    while let Some(row) = result.next().await? {
        if let Ok(label) = row.get::<String>("label") {
            if let Ok(property) = row.get::<String>("property") {
                existing_indices.push((label, property));
            }
        }
    }

    let required_indices = vec![
        ("Address", "address"),
        ("Transaction", "id"),
        ("Cache", "field"),
    ];

    for (label, property) in required_indices {
        let index_exists = existing_indices.iter().any(|(l, p)|
            l == label && p == property
        );

        if !index_exists {
            let query = format!(
                "CREATE INDEX ON :{} ({})",
                label, property
            );
            match graph.run(query.as_str().into()).await {
                Ok(_) => info!("Created index on {}.{}", label, property),
                Err(e) => {
                    error!("Failed to create index on {}.{}: {:?}", label, property, e);
                    return Err(ProcessingError::Neo4jError(e));
                }
            }
        } else {
            info!("Index on {}.{} already exists", label, property);
        }
    }

    info!("Memgraph indices verification completed");
    Ok(())
}

async fn initialize_neo4j_indices(graph: &Graph) -> Result<(), ProcessingError> {
    let check_constraint = "
        SHOW CONSTRAINTS
        YIELD name, labelsOrTypes, properties
        WHERE labelsOrTypes = ['Address']
        AND properties = ['address']
    ";

    let mut result = graph.execute(check_constraint.into()).await?;
    let constraint_exists = result.next().await?.is_some();

    if !constraint_exists {
        // Create constraint only if it doesn't exist
        let constraint_query = "CREATE CONSTRAINT address_unique IF NOT EXISTS FOR (a:Address) REQUIRE a.address IS UNIQUE;";
        match graph.run(constraint_query.into()).await {
            Ok(_) => info!("Created unique constraint on Address.address"),
            Err(e) => {
                error!("Failed to create constraint: {:?}", e);
                return Err(ProcessingError::Neo4jError(e));
            }
        }
    } else {
        info!("Address unique constraint already exists");
    }

    let check_indices = "
        SHOW INDEXES
        YIELD name, labelsOrTypes, properties
    ";

    let mut indices_result = graph.execute(check_indices.into()).await?;
    let mut existing_indices = Vec::new();

    while let Some(row) = indices_result.next().await? {
        let label: String = row.get("labelsOrTypes").unwrap_or_default();
        let property: String = row.get("properties").unwrap_or_default();
        existing_indices.push((label, property));
    }

    let required_indices = vec![
        ("Transaction", "id"),
        ("Cache", "field"),
    ];

    for (label, property) in required_indices {
        let index_exists = existing_indices.iter().any(|(l, p)|
            l.contains(label) && p.contains(property)
        );

        if !index_exists {
            let query = format!(
                "CREATE INDEX IF NOT EXISTS FOR (n:{}) ON (n.{})",
                label, property
            );
            match graph.run(query.as_str().into()).await {
                Ok(_) => info!("Created index on {}.{}", label, property),
                Err(e) => {
                    error!("Failed to create index on {}.{}: {:?}", label, property, e);
                    return Err(ProcessingError::Neo4jError(e));
                }
            }
        } else {
            info!("Index on {}.{} already exists", label, property);
        }
    }

    info!("Neo4j indices and constraints verification completed");
    Ok(())
}

#[derive(Default)]
struct BlockStats {
    deposit_count: i64,
    withdrawal_count: i64,
    transfer_count: i64,
    stake_added_count: i64,
    stake_removed_count: i64,
    balance_set_count: i64,
}

#[derive(Component, Clone, Serialize, Deserialize, Debug, Default)]
struct ProcessingState {
    state: State,
    retry_count: u32,
    last_processed_block: i64,
    polling_mode: bool,
    batch_size: i64,
    current_batch_end: i64,
}

#[derive(PartialEq, Clone, Serialize, Deserialize, Debug, Default)]
enum State {
    #[default]
    Idle,
    FetchingData,
    ProcessingData,
    StoringData,
    Error,
}

#[derive(Component)]
struct AsyncTaskSender {
    tx: mpsc::Sender<AsyncTask>,
}

#[derive(Component)]
struct AsyncTaskReceiver {
    rx: mpsc::Receiver<AsyncResult>,
}

#[derive(Debug, Clone)]
enum AsyncTask {
    FetchData(i64, i64),
    StoreData(BlockData),
    CancelOperation(i64),
    EnterPollingMode,
    Error,
}

#[derive(Debug)]
enum AsyncResult {
    DataFetched(BlockData),
    DataStored(bool),
    OperationCancelled(i64),
    Error(ProcessingError),
    EnterPollingMode,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct BlockData {
    deposits: Nodes<Deposit>,
    withdrawals: Nodes<Withdrawal>,
    transfers: Nodes<Transfer>,
    stakeAddeds: Nodes<StakeAdded>,
    stakeRemoveds: Nodes<StakeRemoved>,
    balanceSets: Nodes<BalanceSet>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Nodes<T> {
    nodes: Vec<T>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Deposit {
    id: String,
    amount: String,
    blockNumber: i64,
    date: String,
    toId: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Withdrawal {
    id: String,
    amount: String,
    blockNumber: i64,
    date: String,
    fromId: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Transfer {
    id: String,
    amount: String,
    blockNumber: i64,
    date: String,
    fromId: String,
    toId: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct StakeAdded {
    id: String,
    amount: String,
    blockNumber: i64,
    date: String,
    fromId: String,
    toId: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct StakeRemoved {
    id: String,
    amount: String,
    blockNumber: i64,
    date: String,
    fromId: String,
    toId: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct BalanceSet {
    id: String,
    amount: String,
    blockNumber: i64,
    date: String,
    whoId: String,
}

use neo4rs::{BoltString};

async fn get_last_processed_block(graph: &Graph) -> Result<i64, ProcessingError>
    {
        let query = "
         MATCH (n:Cache {field: 'max_block_height'})
         RETURN n.value AS last_block
     ";

        let mut result = graph.execute(query.into()).await?;

        if let Some(row) = result.next().await? {
            let last_block: i64 = row.get("last_block").unwrap_or(0);
            Ok(last_block)
        } else {
            Ok(0) // If no Cache node exists, start from block 0
        }
    }

impl Transfer {
    pub fn to_bolt_type(&self) -> BoltType {
        let mut map = BoltMap::new();
        map.put(BoltString::from("id"), BoltType::String(BoltString::from(self.id.clone())));
        map.put(BoltString::from("amount"), BoltType::String(BoltString::from(self.amount.to_string())));
        map.put(BoltString::from("blockNumber"), BoltType::Integer(BoltInteger::from(self.blockNumber)));
        map.put(BoltString::from("date"), BoltType::String(BoltString::from(self.date.clone())));
        map.put(BoltString::from("fromId"), BoltType::String(BoltString::from(self.fromId.clone())));
        map.put(BoltString::from("toId"), BoltType::String(BoltString::from(self.toId.clone())));
        BoltType::Map(map)
    }
}

impl Deposit {
    pub fn to_bolt_type(&self) -> BoltType {
        let mut map = BoltMap::new();
        map.put(BoltString::from("id"), BoltType::String(BoltString::from(self.id.clone())));
        map.put(BoltString::from("amount"), BoltType::String(BoltString::from(self.amount.to_string())));
        map.put(BoltString::from("blockNumber"), BoltType::Integer(BoltInteger::from(self.blockNumber)));
        map.put(BoltString::from("date"), BoltType::String(BoltString::from(self.date.clone())));
        map.put(BoltString::from("toId"), BoltType::String(BoltString::from(self.toId.clone())));
        BoltType::Map(map)
    }
}

impl Withdrawal {
    pub fn to_bolt_type(&self) -> BoltType {
        let mut map = BoltMap::new();
        map.put(BoltString::from("id"), BoltType::String(BoltString::from(self.id.clone())));
        map.put(BoltString::from("amount"), BoltType::String(BoltString::from(self.amount.to_string())));
        map.put(BoltString::from("blockNumber"), BoltType::Integer(BoltInteger::from(self.blockNumber)));
        map.put(BoltString::from("date"), BoltType::String(BoltString::from(self.date.clone())));
        map.put(BoltString::from("fromId"), BoltType::String(BoltString::from(self.fromId.clone())));
        BoltType::Map(map)
    }
}

impl BalanceSet {
    pub fn to_bolt_type(&self) -> BoltType {
        let mut map = BoltMap::new();
        map.put(BoltString::from("id"), BoltType::String(BoltString::from(self.id.clone())));
        map.put(BoltString::from("amount"), BoltType::String(BoltString::from(self.amount.to_string())));
        map.put(BoltString::from("blockNumber"), BoltType::Integer(BoltInteger::from(self.blockNumber)));
        map.put(BoltString::from("date"), BoltType::String(BoltString::from(self.date.clone())));
        map.put(BoltString::from("whoId"), BoltType::String(BoltString::from(self.whoId.clone())));
        BoltType::Map(map)
    }
}

impl StakeAdded {
    pub fn to_bolt_type(&self) -> BoltType {
        let mut map = BoltMap::new();
        map.put(BoltString::from("id"), BoltType::String(BoltString::from(self.id.clone())));
        map.put(BoltString::from("amount"), BoltType::String(BoltString::from(self.amount.to_string())));
        map.put(BoltString::from("blockNumber"), BoltType::Integer(BoltInteger::from(self.blockNumber)));
        map.put(BoltString::from("date"), BoltType::String(BoltString::from(self.date.clone())));
        map.put(BoltString::from("fromId"), BoltType::String(BoltString::from(self.fromId.clone())));
        map.put(BoltString::from("toId"), BoltType::String(BoltString::from(self.toId.clone())));
        BoltType::Map(map)
    }
}

impl StakeRemoved {
    pub fn to_bolt_type(&self) -> BoltType {
        let mut map = BoltMap::new();
        map.put(BoltString::from("id"), BoltType::String(BoltString::from(self.id.clone())));
        map.put(BoltString::from("amount"), BoltType::String(BoltString::from(self.amount.to_string())));
        map.put(BoltString::from("blockNumber"), BoltType::Integer(BoltInteger::from(self.blockNumber as i64)));
        map.put(BoltString::from("date"), BoltType::String(BoltString::from(self.date.clone())));
        map.put(BoltString::from("fromId"), BoltType::String(BoltString::from(self.fromId.clone())));
        map.put(BoltString::from("toId"), BoltType::String(BoltString::from(self.toId.clone())));
        BoltType::Map(map)
    }
}

#[derive(Debug, thiserror::Error)]
enum ProcessingError {
    #[error("Failed to fetch data: {0}")]
    FetchError(String),
    #[error("Failed to process data: {0}")]
    ProcessError(String),
    #[error("Failed to store data: {0}")]
    StoreError(String),
    #[error("Neo4j error: {0}")]
    Neo4jError(#[from] neo4rs::Error),
}

async fn fetch_blocks_data(start_block: i64, end_block: i64) -> Result<BlockData, ProcessingError> {
    let subql_url = std::env::var("SUBQL_URL").expect("SUBQL_URL must be set");
    let client = reqwest::Client::new();
    let query = r#"
    query($startBlock: Int!, $endBlock: Int!) {
        deposits(filter: { blockNumber: { greaterThanOrEqualTo: $startBlock, lessThanOrEqualTo: $endBlock } }, orderBy: [BLOCK_NUMBER_ASC, DATE_ASC]) {
            nodes {
                id
                amount
                blockNumber
                date
                toId
            }
        }
        withdrawals(filter: { blockNumber: { greaterThanOrEqualTo: $startBlock, lessThanOrEqualTo: $endBlock } }, orderBy: [BLOCK_NUMBER_ASC, DATE_ASC]) {
            nodes {
                id
                amount
                blockNumber
                date
                fromId
            }
        }
        transfers(filter: { blockNumber: { greaterThanOrEqualTo: $startBlock, lessThanOrEqualTo: $endBlock } }, orderBy: [BLOCK_NUMBER_ASC, DATE_ASC]) {
            nodes {
                id
                amount
                blockNumber
                date
                fromId
                toId
            }
        }
        stakeAddeds(filter: { blockNumber: { greaterThanOrEqualTo: $startBlock, lessThanOrEqualTo: $endBlock } }, orderBy: [BLOCK_NUMBER_ASC, DATE_ASC]) {
            nodes {
                id
                amount
                blockNumber
                date
                fromId
                toId
            }
        }
        stakeRemoveds(filter: { blockNumber: { greaterThanOrEqualTo: $startBlock, lessThanOrEqualTo: $endBlock } }, orderBy: [BLOCK_NUMBER_ASC, DATE_ASC]) {
            nodes {
                id
                amount
                blockNumber
                date
                fromId
                toId
            }
        }
        balanceSets(filter: { blockNumber: { greaterThanOrEqualTo: $startBlock, lessThanOrEqualTo: $endBlock } }, orderBy: [BLOCK_NUMBER_ASC, DATE_ASC]) {
            nodes {
                id
                amount
                blockNumber
                date
                whoId
            }
        }
    }
    "#;

    let variables = serde_json::json!({
        "startBlock": start_block,
        "endBlock": end_block
    });

    let response = client
        .post(subql_url)
        .json(&serde_json::json!({ "query": query, "variables": variables }))
        .send()
        .await
        .map_err(|e| ProcessingError::FetchError(e.to_string()))?
        .json::<serde_json::Value>()
        .await
        .map_err(|e| ProcessingError::FetchError(e.to_string()))?;

   let block_data: BlockData = serde_json::from_value(response["data"].clone())
        .map_err(|e| ProcessingError::ProcessError(e.to_string()))?;

    Ok(block_data)
}


async fn store_block_data(graph: &Graph, block_data: &BlockData) -> Result<bool, ProcessingError> {
    let max_retries = 3;
    let mut retry_count = 0;
    
    while retry_count < max_retries {
        match try_store_block_data(graph, block_data).await {
            Ok(success) => return Ok(success),
            Err(e) => {
                if let ProcessingError::Neo4jError(ref neo_err) = e {
                    if neo_err.to_string().contains("Cannot resolve conflicting transactions") {
                        retry_count += 1;
                        if retry_count < max_retries {
                            info!("Transaction conflict detected, retrying ({}/{})", retry_count, max_retries);
                            tokio::time::sleep(Duration::from_millis(100 * retry_count as u64)).await;
                            continue;
                        }
                    }
                }
                return Err(e);
            }
        }
    }
    
    Err(ProcessingError::ProcessError("Max retries exceeded for transaction conflicts".to_string()))
}

async fn try_store_block_data(graph: &Graph, block_data: &BlockData) -> Result<bool, ProcessingError> {
    // Check if there's any data to process
    let total_operations = block_data.deposits.nodes.len() +
        block_data.withdrawals.nodes.len() +
        block_data.transfers.nodes.len() +
        block_data.stakeAddeds.nodes.len() +
        block_data.stakeRemoveds.nodes.len() +
        block_data.balanceSets.nodes.len();

    if total_operations == 0 {
        info!("No operations to process in this batch, skipping transaction");
        return Ok(true);
    }

    info!("Starting transaction for blocks {} to {} with {} total operations", 
        block_data.deposits.nodes.first().map(|d| d.blockNumber).unwrap_or(0),
        block_data.deposits.nodes.last().map(|d| d.blockNumber).unwrap_or(0),
        total_operations
    );
    
    let mut txn = graph.start_txn().await?;

    let deposits_bolt: Vec<BoltType> = block_data
        .deposits
        .nodes
        .iter()
        .map(|d| {
            let mut bolt = d.to_bolt_type();
            if let BoltType::Map(ref mut map) = bolt {
                if let Some(BoltType::Integer(amount)) = map.value.get("amount") {
                    map.value.insert("amount".into(), BoltType::Float(neo4rs::BoltFloat { value: amount.value as f64 }));
                }
            }
            bolt
        })
        .collect();

    let withdrawals_bolt: Vec<BoltType> = block_data
        .withdrawals
        .nodes
        .iter()
        .map(|w| {
            let mut bolt = w.to_bolt_type();
            if let BoltType::Map(ref mut map) = bolt {
                if let Some(BoltType::Integer(amount)) = map.value.get("amount") {
                    map.value.insert("amount".into(), BoltType::Float(neo4rs::BoltFloat { value: amount.value as f64 }));
                }
            }
            bolt
        })
        .collect();

    let transfers_bolt: Vec<BoltType> = block_data
        .transfers
        .nodes
        .iter()
        .map(|t| {
            let mut bolt = t.to_bolt_type();
            if let BoltType::Map(ref mut map) = bolt {
                if let Some(BoltType::Integer(amount)) = map.value.get("amount") {
                    map.value.insert("amount".into(), BoltType::Float(neo4rs::BoltFloat { value: amount.value as f64 }));
                }
            }
            bolt
        })
        .collect();

    let stake_addeds_bolt: Vec<BoltType> = block_data
        .stakeAddeds
        .nodes
        .iter()
        .map(|sa| {
            let mut bolt = sa.to_bolt_type();
            if let BoltType::Map(ref mut map) = bolt {
                if let Some(BoltType::Integer(amount)) = map.value.get("amount") {
                    map.value.insert("amount".into(), BoltType::Float(neo4rs::BoltFloat { value: amount.value as f64 }));
                }
            }
            bolt
        })
        .collect();

    let stake_removeds_bolt: Vec<BoltType> = block_data
        .stakeRemoveds
        .nodes
        .iter()
        .map(|sr| {
            let mut bolt = sr.to_bolt_type();
            if let BoltType::Map(ref mut map) = bolt {
                if let Some(BoltType::Integer(amount)) = map.value.get("amount") {
                    map.value.insert("amount".into(), BoltType::Float(neo4rs::BoltFloat { value: amount.value as f64 }));
                }
            }
            bolt
        })
        .collect();

    let balance_sets_bolt: Vec<BoltType> = block_data
        .balanceSets
        .nodes
        .iter()
        .map(|bs| {
            let mut bolt = bs.to_bolt_type();
            if let BoltType::Map(ref mut map) = bolt {
                if let Some(BoltType::Integer(amount)) = map.value.get("amount") {
                    map.value.insert("amount".into(), BoltType::Float(neo4rs::BoltFloat { value: amount.value as f64 }));
                }
            }
            bolt
        })
        .collect();

    let max_block = [
        block_data.deposits.nodes.iter().map(|x| x.blockNumber).max(),
        block_data.withdrawals.nodes.iter().map(|x| x.blockNumber).max(),
        block_data.transfers.nodes.iter().map(|x| x.blockNumber).max(),
        block_data.stakeAddeds.nodes.iter().map(|x| x.blockNumber).max(),
        block_data.stakeRemoveds.nodes.iter().map(|x| x.blockNumber).max(),
        block_data.balanceSets.nodes.iter().map(|x| x.blockNumber).max(),
    ]
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(0);

    let min_block = [
        block_data.deposits.nodes.iter().map(|x| x.blockNumber).min(),
        block_data.withdrawals.nodes.iter().map(|x| x.blockNumber).min(),
        block_data.transfers.nodes.iter().map(|x| x.blockNumber).min(),
        block_data.stakeAddeds.nodes.iter().map(|x| x.blockNumber).min(),
        block_data.stakeRemoveds.nodes.iter().map(|x| x.blockNumber).min(),
        block_data.balanceSets.nodes.iter().map(|x| x.blockNumber).min(),
    ]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(0);

    // Create stats for ALL blocks in range
    let mut block_stats: HashMap<i64, BlockStats> =
        (min_block..=max_block)
            .map(|block_num| (block_num, BlockStats::default()))
            .collect();

    // Update stats for blocks with transactions
    for deposit in &block_data.deposits.nodes {
        if let Some(stats) = block_stats.get_mut(&deposit.blockNumber) {
            stats.deposit_count += 1;
        }
    }

    for withdrawal in &block_data.withdrawals.nodes {
        if let Some(stats) = block_stats.get_mut(&withdrawal.blockNumber) {
            stats.withdrawal_count += 1;
        }
    }

    for transfer in &block_data.transfers.nodes {
        if let Some(stats) = block_stats.get_mut(&transfer.blockNumber) {
            stats.transfer_count += 1;
        }
    }

    for stake_add in &block_data.stakeAddeds.nodes {
        if let Some(stats) = block_stats.get_mut(&stake_add.blockNumber) {
            stats.stake_added_count += 1;
        }
    }

    for stake_remove in &block_data.stakeRemoveds.nodes {
        if let Some(stats) = block_stats.get_mut(&stake_remove.blockNumber) {
            stats.stake_removed_count += 1;
        }
    }

    for balance_set in &block_data.balanceSets.nodes {
        if let Some(stats) = block_stats.get_mut(&balance_set.blockNumber) {
            stats.balance_set_count += 1;
        }
    }

    // Convert block stats to BoltType
    let blocks_bolt: Vec<BoltType> = block_stats
        .into_iter()
        .map(|(block_number, stats)| {
            let total = stats.deposit_count + stats.withdrawal_count + stats.transfer_count +
                stats.stake_added_count + stats.stake_removed_count + stats.balance_set_count;

            BoltType::Map(BoltMap {
                value: HashMap::from([
                    ("height".into(), BoltType::Integer(block_number.into())),
                    ("deposit_count".into(), BoltType::Integer(stats.deposit_count.into())),
                    ("withdrawal_count".into(), BoltType::Integer(stats.withdrawal_count.into())),
                    ("transfer_count".into(), BoltType::Integer(stats.transfer_count.into())),
                    ("stake_added_count".into(), BoltType::Integer(stats.stake_added_count.into())),
                    ("stake_removed_count".into(), BoltType::Integer(stats.stake_removed_count.into())),
                    ("balance_set_count".into(), BoltType::Integer(stats.balance_set_count.into())),
                    ("total_txs".into(), BoltType::Integer(total.into())),
                ])
            })
        })
        .collect();

    let query = "
    // First create all addresses including system in a single operation
    MERGE (system:Address {address: 'system'})
    WITH system
    UNWIND $deposits + $withdrawals + $transfers + $stake_addeds + $stake_removeds + $balance_sets AS tx
    WITH system, tx, 
         CASE 
           WHEN tx.toId IS NOT NULL THEN tx.toId 
           WHEN tx.fromId IS NOT NULL THEN tx.fromId
           WHEN tx.whoId IS NOT NULL THEN tx.whoId
         END AS addr
    WHERE addr IS NOT NULL
    MERGE (a:Address {address: addr})
    
    // Process all transactions in a single pass
    WITH system, collect(DISTINCT tx) as all_txs
    UNWIND all_txs as tx
    WITH system, tx,
         CASE tx
           WHEN tx IN $deposits THEN 'deposit'
           WHEN tx IN $withdrawals THEN 'withdrawal'
           WHEN tx IN $transfers THEN 'transfer'
           WHEN tx IN $stake_addeds THEN 'stake_added'
           WHEN tx IN $stake_removeds THEN 'stake_removed'
           WHEN tx IN $balance_sets THEN 'balance_set'
         END as tx_type,
         CASE
           WHEN tx IN $deposits THEN tx.toId
           WHEN tx IN $withdrawals THEN tx.fromId
           WHEN tx IN $transfers THEN tx.fromId
           WHEN tx IN $stake_addeds THEN tx.fromId
           WHEN tx IN $stake_removeds THEN tx.fromId
           WHEN tx IN $balance_sets THEN tx.whoId
         END as from_addr,
         CASE
           WHEN tx IN $deposits THEN null
           WHEN tx IN $withdrawals THEN null
           WHEN tx IN $transfers THEN tx.toId
           WHEN tx IN $stake_addeds THEN tx.toId
           WHEN tx IN $stake_removeds THEN tx.toId
           WHEN tx IN $balance_sets THEN null
         END as to_addr
    
    WITH system, tx, tx_type, from_addr, to_addr,
         CASE
           WHEN tx_type IN ['deposit', 'balance_set'] THEN [system, null]
           WHEN tx_type = 'withdrawal' THEN [null, system]
           ELSE [null, null]
         END as system_endpoints
    
    MATCH (start:Address {address: COALESCE(system_endpoints[0], from_addr)})
    MATCH (end:Address {address: COALESCE(system_endpoints[1], COALESCE(to_addr, from_addr))})
    
    MERGE (start)-[tr:TRANSACTION {id: tx.id}]->(end)
    ON CREATE SET 
        tr.type = tx_type,
        tr.amount = toFloat(tx.amount),
        tr.block_height = tx.blockNumber,
        tr.timestamp = datetime(tx.date + 'Z')
    ON MATCH SET 
        tr.amount = toFloat(tx.amount),
        tr.block_height = tx.blockNumber,
        tr.timestamp = datetime(tx.date + 'Z')
    
    RETURN count(*) AS txs_operations
";

    let blocks_query = "\
// Create or update blocks
    UNWIND $blocks AS block
        MERGE (b:Block {height: block.height})
        SET b.deposit_count = block.deposit_count,
            b.withdrawal_count = block.withdrawal_count,
            b.transfer_count = block.transfer_count,
            b.stake_added_count = block.stake_added_count,
            b.stake_removed_count = block.stake_removed_count,
            b.balance_set_count = block.balance_set_count,
            b.total_txs = block.total_txs

    RETURN count(*) AS blk_operations
    ";

    let data_query = neo4rs::Query::new(query.to_string())
        .param("deposits", BoltType::List(BoltList::from(deposits_bolt)))
        .param("withdrawals", BoltType::List(BoltList::from(withdrawals_bolt)))
        .param("transfers", BoltType::List(BoltList::from(transfers_bolt)))
        .param("stake_addeds", BoltType::List(BoltList::from(stake_addeds_bolt)))
        .param("stake_removeds", BoltType::List(BoltList::from(stake_removeds_bolt)))
        .param("balance_sets", BoltType::List(BoltList::from(balance_sets_bolt)));

    txn.run(data_query).await?;

    let blocks_query = neo4rs::Query::new(blocks_query.parse().unwrap())
        .param("blocks", BoltType::List(BoltList::from(blocks_bolt)));

    if max_block > 0 {
        let cache_query = "
            MERGE (n:Cache {field: 'max_block_height'})
            WITH n, n.value as current_value, $new_value as new_value
            SET n.value = CASE
                WHEN current_value IS NULL OR new_value > current_value
                THEN new_value
                ELSE current_value
            END
        ";

        let cache_update = neo4rs::Query::new(cache_query.to_string())
            .param("new_value", BoltType::Integer(max_block.into()));

        txn.run(cache_update).await?;
    }

    txn.run(blocks_query).await?;

    match txn.commit().await {
        Ok(_) => info!("Transaction committed successfully"),
        Err(e) => {
            let error_msg = e.to_string();
            error!("Transaction error occurred: {}", error_msg);
            
            // Always log operation details on error
            error!("Transaction conflict details:");
            error!("- Deposits: {} operations", block_data.deposits.nodes.len());
            error!("- Withdrawals: {} operations", block_data.withdrawals.nodes.len());
            error!("- Transfers: {} operations", block_data.transfers.nodes.len());
            error!("- Stake Adds: {} operations", block_data.stakeAddeds.nodes.len());
            error!("- Stake Removes: {} operations", block_data.stakeRemoveds.nodes.len());
            error!("- Balance Sets: {} operations", block_data.balanceSets.nodes.len());
            
            // Collect and log affected addresses
            let mut affected_addresses = Vec::new();
            for d in &block_data.deposits.nodes {
                affected_addresses.push(d.toId.clone());
            }
            for w in &block_data.withdrawals.nodes {
                affected_addresses.push(w.fromId.clone());
            }
            for t in &block_data.transfers.nodes {
                affected_addresses.push(t.fromId.clone());
                affected_addresses.push(t.toId.clone());
            }
            for sa in &block_data.stakeAddeds.nodes {
                affected_addresses.push(sa.fromId.clone());
                affected_addresses.push(sa.toId.clone());
            }
            for sr in &block_data.stakeRemoveds.nodes {
                affected_addresses.push(sr.fromId.clone());
                affected_addresses.push(sr.toId.clone());
            }
            for bs in &block_data.balanceSets.nodes {
                affected_addresses.push(bs.whoId.clone());
            }
            affected_addresses.sort();
            affected_addresses.dedup();
            error!("- Affected addresses ({}): {:?}", affected_addresses.len(), affected_addresses);
            
            return Err(ProcessingError::Neo4jError(e));
        }
    };

    info!("Stored batch with {} deposits, {} withdrawals, {} transfers, {} stake adds, {} stake removes, {} balance sets. Max block: {}",
        block_data.deposits.nodes.len(),
        block_data.withdrawals.nodes.len(),
        block_data.transfers.nodes.len(),
        block_data.stakeAddeds.nodes.len(),
        block_data.stakeRemoveds.nodes.len(),
        block_data.balanceSets.nodes.len(),
        max_block
    );

    Ok(true)
}


#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt::init();

    let db_type = env::var("DB_TYPE").unwrap_or_else(|_| "neo4j".to_string());

    let uri = std::env::var("GRAPH_DB_URI").expect("NEO4J_URI must be set");
    let user = std::env::var("GRAPH_DB_USER").expect("NEO4J_USER must be set");
    let password = std::env::var("GRAPH_DB_PASSWORD").expect("NEO4J_PASSWORD must be set");

    let config = ConfigBuilder::default()
        .uri(uri)
        .user(user)
        .password(password)
        .db("memgraph")
        .build()?;

    let graph = Arc::new(Graph::connect(config).await?);

    // Initialize indices based on database type
    initialize_indices(&graph, &db_type).await?;

    let mut world = World::new();

    world.add::<ProcessingState>();

    let (task_tx, mut task_rx) = mpsc::channel::<AsyncTask>(100);
    let (result_tx, result_rx) = mpsc::channel::<AsyncResult>(100);

    let last_processed_block = get_last_processed_block(&graph).await?;

    world.entity()
        .set(ProcessingState {
            state: State::Idle,
            retry_count: 0,
            last_processed_block,
            polling_mode: false,
            batch_size: 5,
            current_batch_end: 0
        })
        .set(AsyncTaskSender { tx: task_tx.clone() })
        .set(AsyncTaskReceiver { rx: result_rx });

    let graph_clone = Arc::clone(&graph);
    tokio::spawn(async move {
        while let Some(task) = task_rx.recv().await {
            match task {
                AsyncTask::FetchData(start_block, end_block) => {
                    match timeout(Duration::from_secs(5), fetch_blocks_data(start_block, end_block)).await {
                        Ok(result) => {
                            match result {
                                Ok(data) => result_tx.send(AsyncResult::DataFetched(data)).await.unwrap(),
                                Err(e) => result_tx.send(AsyncResult::Error(e)).await.unwrap(),
                            }
                        }
                        Err(_) => {
                            result_tx.send(AsyncResult::Error(ProcessingError::FetchError("Timeout".to_string()))).await.unwrap();
                        }
                    }
                }
                AsyncTask::StoreData(data) => {
                    match timeout(Duration::from_secs(5), store_block_data(&graph_clone, &data)).await {
                        Ok(result) => {
                            match result {
                                Ok(success) => result_tx.send(AsyncResult::DataStored(success)).await.unwrap(),
                                Err(e) => result_tx.send(AsyncResult::Error(e)).await.unwrap(),
                            }
                        }
                        Err(_) => {
                            result_tx.send(AsyncResult::Error(ProcessingError::StoreError("Timeout".to_string()))).await.unwrap();
                        }
                    }
                }
                AsyncTask::CancelOperation(block_height) => {
                    result_tx.send(AsyncResult::OperationCancelled(block_height)).await.unwrap();
                }
                AsyncTask::EnterPollingMode => {
                    result_tx.send(AsyncResult::EnterPollingMode).await.unwrap();
                }
                AsyncTask::Error => {
                    result_tx.send(AsyncResult::Error(ProcessingError::ProcessError("Generic error".to_string()))).await.unwrap();
                }
            }
        }
    });

    let handle_async_sys = world
        .system::<(&mut ProcessingState, &mut AsyncTaskReceiver, &AsyncTaskSender)>()
        .each_entity(|_, (state, receiver, sender)| {
            if let Ok(result) = receiver.rx.try_recv() {
                let span = span!(Level::INFO, "handle_async_result", state = ?state.state);
                let _enter = span.enter();

                match result {
                    AsyncResult::DataFetched(block_data) => {
                        info!("Data fetched successfully for blocks: {} to {}",
                          state.last_processed_block, state.current_batch_end);
                        state.state = State::StoringData;
                        if let Err(e) = sender.tx.try_send(AsyncTask::StoreData(block_data)) {
                            error!("Failed to send store task: {}", e);
                            state.state = State::Error;
                        }
                    },
                    AsyncResult::DataStored(success) => {
                        if success {
                            info!("Data stored successfully for blocks: {} to {}",
                              state.last_processed_block, state.current_batch_end);
                            state.state = State::Idle;
                            state.last_processed_block = state.current_batch_end + 1;
                            state.retry_count = 0;

                            // Check if we need to enter polling mode
                            let sender_clone = sender.tx.clone();
                            let current_block = state.last_processed_block;
                            tokio::spawn(async move {
                                match get_latest_block_height().await {
                                    Ok(latest_block) => {
                                        if current_block > latest_block {
                                            sender_clone.send(AsyncTask::EnterPollingMode).await.unwrap();
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to get latest block height: {:?}", e);
                                        sender_clone.send(AsyncTask::Error).await.unwrap();
                                    }
                                }
                            });
                        } else {
                            error!("Failed to store data for batch: {} to {}",
                               state.last_processed_block, state.current_batch_end);
                            state.state = State::Error;
                        }
                    },
                    AsyncResult::OperationCancelled(block_height) => {
                        info!("Operation cancelled for block batch ending at: {}", block_height);
                        state.state = State::Idle;
                    },
                    AsyncResult::Error(err) => {
                        error!("Error occurred processing batch {} to {}: {:?}",
                           state.last_processed_block, state.current_batch_end, err);
                        state.state = State::Error;
                    },
                    AsyncResult::EnterPollingMode => {
                        info!("Entered polling mode. Waiting for new blocks...");
                        state.polling_mode = true;
                        state.state = State::Idle;
                    },
                }
            }
        });


    let start_sys = world
        .system::<(&mut ProcessingState, &AsyncTaskSender)>()
        .each_entity(|_, (state, sender)| {
            if state.state == State::Idle {
                let span = span!(Level::INFO, "start_processing", block = state.last_processed_block);
                let _enter = span.enter();

                if state.polling_mode {
                    let sender_clone = sender.tx.clone();
                    let current_block = state.last_processed_block;
                    let batch_size = state.batch_size;
                    tokio::spawn(async move {
                        match get_latest_block_height().await {
                            Ok(latest_block) => {
                                if latest_block > current_block {
                                    let end_block = std::cmp::min(
                                        current_block + batch_size - 1,
                                        latest_block
                                    );
                                    sender_clone.send(AsyncTask::FetchData(current_block, end_block))
                                        .await.unwrap();
                                } else {
                                    debug!("No new blocks found. Current: {}, Latest: {}",
                                      current_block, latest_block);
                                }
                            }
                            Err(e) => {
                                error!("Failed to get latest block height: {:?}", e);
                                sender_clone.send(AsyncTask::Error).await.unwrap();
                            }
                        }
                    });
                } else {
                    let next_batch_end = state.last_processed_block + state.batch_size - 1;
                    info!("Starting processing for block batch: {} to {}",
                      state.last_processed_block, next_batch_end);

                    state.current_batch_end = next_batch_end;
                    state.state = State::FetchingData;

                    if let Err(e) = sender.tx.try_send(
                        AsyncTask::FetchData(state.last_processed_block, next_batch_end)
                    ) {
                        error!("Failed to send fetch task: {}", e);
                        state.state = State::Error;
                    }
                }
            }
        });

    let err_sys = world
        .system::<(&mut ProcessingState, &AsyncTaskSender)>()
        .each_entity(|_, (state, sender)| {
            if state.state == State::Error {
                let span = span!(Level::ERROR, "error_handling",
                start_block = state.last_processed_block,
                end_block = state.current_batch_end);
                let _enter = span.enter();

                error!("Handling error for block batch: {} to {}",
                   state.last_processed_block, state.current_batch_end);
                state.retry_count += 1;

                if state.retry_count > 3 {
                    warn!("Max retries reached for batch starting at block: {}, moving to next batch",
                      state.last_processed_block);
                    state.state = State::Idle;
                    state.retry_count = 0;

                    state.last_processed_block = state.current_batch_end + 1;
                    state.current_batch_end = 0;  // Will be set in start_sys

                } else {
                    let retry_delay = 2u64.pow(state.retry_count - 1);
                    info!("Retrying operation for batch {} to {}, attempt {}, waiting {}s",
                      state.last_processed_block, state.current_batch_end,
                      state.retry_count, retry_delay);

                    let adjusted_batch_end = if state.retry_count > 1 {
                        let reduced_size = (state.current_batch_end - state.last_processed_block + 1) / 2;
                        state.last_processed_block + reduced_size - 1
                    } else {
                        state.current_batch_end
                    };

                    state.current_batch_end = adjusted_batch_end;
                    state.state = State::FetchingData;

                    let sender_clone = sender.tx.clone();
                    let start_block = state.last_processed_block;
                    let end_block = adjusted_batch_end;

                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(retry_delay)).await;
                        if let Err(e) = sender_clone.try_send(AsyncTask::FetchData(start_block, end_block)) {
                            error!("Failed to send retry task: {}", e);
                        }
                    });
                }
            }
        });


    let mut interval = tokio::time::interval(Duration::from_millis(16));
    loop {
        interval.tick().await;

        start_sys.run();
        err_sys.run();
        handle_async_sys.run();

        world.app()
            .enable_stats(true)
            .enable_rest(27750)
            .run();

        world.progress();
    }
}
async fn get_latest_block_height() -> Result<i64, ProcessingError> {
    let subql_url = std::env::var("SUBQL_URL").expect("SUBQL_URL must be set");
    let client = reqwest::Client::new();
    let query = r#"
        query {
            transfers(orderBy: BLOCK_NUMBER_DESC, first: 1) {
                nodes {
                    blockNumber
                }
            }
        }
    "#;

    let response = client
        .post(subql_url)
        .json(&serde_json::json!({ "query": query }))
        .send()
        .await
        .map_err(|e| ProcessingError::FetchError(e.to_string()))?
        .json::<serde_json::Value>()
        .await
        .map_err(|e| ProcessingError::FetchError(e.to_string()))?;

    let latest_block = response["data"]["transfers"]["nodes"][0]["blockNumber"]
        .as_i64()
        .ok_or_else(|| ProcessingError::ProcessError("Failed to get latest block number".to_string()))?;

    Ok(latest_block)
}
