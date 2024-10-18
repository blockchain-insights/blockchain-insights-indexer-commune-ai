use std::sync::Arc;
use anyhow::{Context, Result};
use flecs_ecs::prelude::flecs::pipeline::OnUpdate;
use flecs_ecs::prelude::*;
use serde::{Deserialize, Serialize};
use dotenv::dotenv;
use neo4rs::{BoltInteger, BoltList, BoltMap, BoltType, Error, Graph, Node};
use reqwest;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use tracing::{debug, error, info, span, warn, Level};

#[derive(Component, Clone, Serialize, Deserialize, Debug, Default)]
struct ProcessingState {
    state: State,
    retry_count: u32,
    last_processed_block: i64,
    polling_mode: bool,
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
    FetchData(i64),
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

async fn fetch_block_data(block_height: i64) -> Result<BlockData, ProcessingError> {
    let subql_url = std::env::var("SUBQL_URL").expect("SUBQL_URL must be set");
    let client = reqwest::Client::new();
    let query = format!(
        r#"
        query {{
            deposits(filter: {{ blockNumber: {{ equalTo: {} }} }}, orderBy: [BLOCK_NUMBER_ASC, DATE_ASC]) {{
                nodes {{
                    id
                    amount
                    blockNumber
                    date
                    toId
                }}
            }}
            withdrawals(filter: {{ blockNumber: {{ equalTo: {} }} }}, orderBy: [BLOCK_NUMBER_ASC, DATE_ASC]) {{
                nodes {{
                    id
                    amount
                    blockNumber
                    date
                    fromId
                }}
            }}
            transfers(filter: {{ blockNumber: {{ equalTo: {} }} }}, orderBy: [BLOCK_NUMBER_ASC, DATE_ASC]) {{
                nodes {{
                    id
                    amount
                    blockNumber
                    date
                    fromId
                    toId
                }}
            }}
            stakeAddeds(filter: {{ blockNumber: {{ equalTo: {} }} }}, orderBy: [BLOCK_NUMBER_ASC, DATE_ASC]) {{
                nodes {{
                    id
                    amount
                    blockNumber
                    date
                    fromId
                    toId
                }}
            }}
            stakeRemoveds(filter: {{ blockNumber: {{ equalTo: {} }} }}, orderBy: [BLOCK_NUMBER_ASC, DATE_ASC]) {{
                nodes {{
                    id
                    amount
                    blockNumber
                    date
                    fromId
                    toId
                }}
            }}
            balanceSets(filter: {{ blockNumber: {{ equalTo: {} }} }}, orderBy: [BLOCK_NUMBER_ASC, DATE_ASC]) {{
                nodes {{
                    id
                    amount
                    blockNumber
                    date
                    whoId
                }}
            }}
        }}
        "#,
        block_height, block_height, block_height, block_height, block_height, block_height
    );

    let response = client
        .post(subql_url)
        .json(&serde_json::json!({ "query": query }))
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
    let mut txn = graph.start_txn().await?;

    let deposits_bolt: Vec<BoltType> = block_data
        .deposits
        .nodes
        .iter()
        .map(|d| d.to_bolt_type())
        .collect();
    let withdrawals_bolt: Vec<BoltType> = block_data
        .withdrawals
        .nodes
        .iter()
        .map(|w| w.to_bolt_type())
        .collect();
    let transfers_bolt: Vec<BoltType> = block_data
        .transfers
        .nodes
        .iter()
        .map(|t| t.to_bolt_type())
        .collect();
    let stake_addeds_bolt: Vec<BoltType> = block_data
        .stakeAddeds
        .nodes
        .iter()
        .map(|sa| sa.to_bolt_type())
        .collect();
    let stake_removeds_bolt: Vec<BoltType> = block_data
        .stakeRemoveds
        .nodes
        .iter()
        .map(|sr| sr.to_bolt_type())
        .collect();
    let balance_sets_bolt: Vec<BoltType> = block_data
        .balanceSets
        .nodes
        .iter()
        .map(|bs| bs.to_bolt_type())
        .collect();

    let query = "
        // Process deposits
        UNWIND $deposits AS deposit
        MERGE (t:Transaction {id: deposit.id})
        SET t.type = 'deposit',
            t.amount = deposit.amount,
            t.blockNumber = deposit.blockNumber,
            t.date = deposit.date
        MERGE (a:Address {address: deposit.toId})
        MERGE (t)-[:DEPOSITED_TO]->(a)

        MERGE (n:Cache {field: 'max_block_height'})
        SET n.value = deposit.blockNumber

        WITH 1 as dummy

        // Process withdrawals
        UNWIND $withdrawals AS withdrawal
        MERGE (t:Transaction {id: withdrawal.id})
        SET t.type = 'withdrawal',
            t.amount = withdrawal.amount,
            t.blockNumber = withdrawal.blockNumber,
            t.date = withdrawal.date
        MERGE (a:Address {address: withdrawal.fromId})
        MERGE (a)-[:WITHDREW]->(t)

        WITH 1 as dummy

        // Process transfers
        UNWIND $transfers AS transfer
        MERGE (t:Transaction {id: transfer.id})
        SET t.type = 'transfer',
            t.amount = transfer.amount,
            t.blockNumber = transfer.blockNumber,
            t.date = transfer.date
        MERGE (from:Address {address: transfer.fromId})
        MERGE (to:Address {address: transfer.toId})
        MERGE (from)-[:SENT]->(t)
        MERGE (t)-[:RECEIVED_BY]->(to)

        WITH 1 as dummy

        // Process stake additions
        UNWIND $stake_addeds AS stake_add
        MERGE (t:Transaction {id: stake_add.id})
        SET t.type = 'stake_added',
            t.amount = stake_add.amount,
            t.blockNumber = stake_add.blockNumber,
            t.date = stake_add.date
        MERGE (from:Address {address: stake_add.fromId})
        MERGE (to:Address {address: stake_add.toId})
        MERGE (from)-[:ADDED_STAKE]->(t)
        MERGE (t)-[:STAKE_ADDED_TO]->(to)

        WITH 1 as dummy

        // Process stake removals
        UNWIND $stake_removeds AS stake_remove
        MERGE (t:Transaction {id: stake_remove.id})
        SET t.type = 'stake_removed',
            t.amount = stake_remove.amount,
            t.blockNumber = stake_remove.blockNumber,
            t.date = stake_remove.date
        MERGE (from:Address {address: stake_remove.fromId})
        MERGE (to:Address {address: stake_remove.toId})
        MERGE (from)-[:REMOVED_STAKE]->(t)
        MERGE (t)-[:STAKE_REMOVED_FROM]->(to)

        WITH 1 as dummy

        // Process balance sets
        UNWIND $balance_sets AS balance_set
        MERGE (t:Transaction {id: balance_set.id})
        SET t.type = 'balance_set',
            t.amount = balance_set.amount,
            t.blockNumber = balance_set.blockNumber,
            t.date = balance_set.date
        MERGE (a:Address {address: balance_set.whoId})
        MERGE (a)-[:BALANCE_SET]->(t)
    ";

    let query = neo4rs::Query::new(query.to_string())
        .param("deposits", BoltType::List(BoltList::from(deposits_bolt)))
        .param("withdrawals", BoltType::List(BoltList::from(withdrawals_bolt)))
        .param("transfers", BoltType::List(BoltList::from(transfers_bolt)))
        .param("stake_addeds", BoltType::List(BoltList::from(stake_addeds_bolt)))
        .param("stake_removeds", BoltType::List(BoltList::from(stake_removeds_bolt)))
        .param("balance_sets", BoltType::List(BoltList::from(balance_sets_bolt)));

    txn.run(query).await?;
    txn.commit().await?;
    Ok(true)
}


#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt::init();

    let uri = std::env::var("NEO4J_URI").expect("NEO4J_URI must be set");
    let user = std::env::var("NEO4J_USER").expect("NEO4J_USER must be set");
    let password = std::env::var("NEO4J_PASSWORD").expect("NEO4J_PASSWORD must be set");

    let graph = Arc::new(Graph::new(uri, user, password).await?);

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
        })
        .set(AsyncTaskSender { tx: task_tx.clone() })
        .set(AsyncTaskReceiver { rx: result_rx });

    let graph_clone = Arc::clone(&graph);
    tokio::spawn(async move {
        while let Some(task) = task_rx.recv().await {
            match task {
                AsyncTask::FetchData(block_height) => {
                    match timeout(Duration::from_secs(5), fetch_block_data(block_height)).await {
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
                        info!("Data fetched successfully for block: {}", state.last_processed_block);
                        state.state = State::StoringData;
                        if let Err(e) = sender.tx.try_send(AsyncTask::StoreData(block_data)) {
                            error!("Failed to send store task: {}", e);
                            state.state = State::Error;
                        }
                    },
                    AsyncResult::DataStored(success) => {
                        if success {
                            info!("Data stored successfully for block: {}", state.last_processed_block);
                            state.state = State::Idle;
                            state.last_processed_block += 1;
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
                            error!("Failed to store data for block: {}", state.last_processed_block);
                            state.state = State::Error;
                        }
                    },
                    AsyncResult::OperationCancelled(block_height) => {
                        info!("Operation cancelled for block: {}", block_height);
                        state.state = State::Idle;
                    },
                    AsyncResult::Error(err) => {
                        error!("Error occurred: {:?}", err);
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
                    // Check for new blocks
                    let sender_clone = sender.tx.clone();
                    let current_block = state.last_processed_block;
                    tokio::spawn(async move {
                        match get_latest_block_height().await {
                            Ok(latest_block) => {
                                if latest_block > current_block {
                                    sender_clone.send(AsyncTask::FetchData(current_block + 1)).await.unwrap();
                                } else {
                                    debug!("No new blocks found. Current: {}, Latest: {}", current_block, latest_block);
                                }
                            }
                            Err(e) => {
                                error!("Failed to get latest block height: {:?}", e);
                                sender_clone.send(AsyncTask::Error).await.unwrap();
                            }
                        }
                    });
                } else {
                    info!("Starting processing for block: {}", state.last_processed_block);
                    state.state = State::FetchingData;
                    if let Err(e) = sender.tx.try_send(AsyncTask::FetchData(state.last_processed_block)) {
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
                let span = span!(Level::ERROR, "error_handling", block = state.last_processed_block);
                let _enter = span.enter();

                error!("Handling error for block: {}", state.last_processed_block);
                state.retry_count += 1;
                if state.retry_count > 3 {
                    warn!("Max retries reached for block: {}, moving to next block", state.last_processed_block);
                    state.state = State::Idle;
                    state.retry_count = 0;
                    state.last_processed_block += 1;
                } else {
                    info!("Retrying operation for block: {}, attempt {}", state.last_processed_block, state.retry_count);
                    state.state = State::FetchingData;
                    if let Err(e) = sender.tx.try_send(AsyncTask::FetchData(state.last_processed_block)) {
                        error!("Failed to send retry task: {}", e);
                    }
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
