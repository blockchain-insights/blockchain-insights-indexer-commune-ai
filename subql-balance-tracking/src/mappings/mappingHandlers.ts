import {
  SubstrateExtrinsic,
  SubstrateEvent,
  SubstrateBlock,
} from "@subql/types";
import { BalanceChange, Block } from "../types";
import { Balance } from "@polkadot/types/interfaces";

export async function handleBlock(block: SubstrateBlock): Promise<void> {
  const blockNumber: number = block.block.header.number.toNumber();

  const blockObj = Block.create({
    id: `${block.block.header.number.toNumber()}`,
    block_height: blockNumber,
    timestamp: block.timestamp
  });

  await Promise.all([blockObj.save()]);
}

export async function handleEvent(event: SubstrateEvent): Promise<void> {
  logger.info(
    `New transfer event found at block ${event.block.block.header.number.toString()}`
  );

  const {
    event: {
      data: [from, to, amount],
    },
  } = event;

  const blockNumber: number = event.block.block.header.number.toNumber();

  const balanceChangeFrom = BalanceChange.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}-1`,
    block_height: blockNumber,
    timestamp: event.block.timestamp,
    event: "transfer - from",
    address: from.toString(),
    balance: -(amount as Balance).toBigInt(),
  });

  const balanceChangeTo = BalanceChange.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}-2`,
    block_height: blockNumber,
    timestamp: event.block.timestamp,
    event: "transfer - to",
    address: to.toString(),
    balance: (amount as Balance).toBigInt(),
  });


  await Promise.all([balanceChangeFrom.save(), balanceChangeTo.save()]);
}

export async function handleWithdrawal(event: SubstrateEvent): Promise<void> {
  logger.info(
      `New withdrawal event found at block ${event.block.block.header.number.toString()}`
  );

  // Get data from the event
  // The balances.transfer event has the following payload \[from, to, value\]
  // logger.info(JSON.stringify(event));
  const {
    event: {
      data: [from, amount],
    },
  } = event;

  const blockNumber: number = event.block.block.header.number.toNumber();

  const balanceChange = BalanceChange.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}`,
    block_height: blockNumber,
    timestamp: event.block.timestamp,
    event: "withdrawal",
    address: from.toString(),
    balance: -(amount as Balance).toBigInt(),
  });

  await Promise.all([balanceChange.save()]);
}

export async function handleStakeAdded(event: SubstrateEvent): Promise<void> {
  logger.info(
      `New stake adding event found at block ${event.block.block.header.number.toString()}`
  );

  // Get data from the event
  // The balances.transfer event has the following payload \[from, to, value\]
  // logger.info(JSON.stringify(event));
  const {
    event: {
      data: [from, to, amount],
    },
  } = event;

  const blockNumber: number = event.block.block.header.number.toNumber();

  const balanceChangeFrom = BalanceChange.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}-1`,
    block_height: blockNumber,
    timestamp: event.block.timestamp,
    event: "stakeAdded - from",
    address: from.toString(),
    balance: -(amount as Balance).toBigInt(),
  });

  const balanceChangeTo = BalanceChange.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}-2`,
    block_height: blockNumber,
    timestamp: event.block.timestamp,
    event: "stakeAdded - to",
    address: to.toString(),
    balance: (amount as Balance).toBigInt(),
  });


  await Promise.all([balanceChangeFrom.save(), balanceChangeTo.save()]);
}

export async function handleStakeRemoved(event: SubstrateEvent): Promise<void> {
  logger.info(
      `New stake removing event found at block ${event.block.block.header.number.toString()}`
  );

  // Get data from the event
  // The balances.transfer event has the following payload \[from, to, value\]
  // logger.info(JSON.stringify(event));
  const {
    event: {
      data: [from, to, amount],
    },
  } = event;

  const blockNumber: number = event.block.block.header.number.toNumber();

  const balanceChangeFrom = BalanceChange.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}-1`,
    block_height: blockNumber,
    timestamp: event.block.timestamp,
    event: "stakeRemoved - from",
    address: from.toString(),
    balance: -(amount as Balance).toBigInt(),
  });

  const balanceChangeTo = BalanceChange.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}-2`,
    block_height: blockNumber,
    timestamp: event.block.timestamp,
    event: "stakeRemoved - to",
    address: to.toString(),
    balance: (amount as Balance).toBigInt(),
  });


  await Promise.all([balanceChangeFrom.save(), balanceChangeTo.save()]);
}

export async function handleDeposit(event: SubstrateEvent): Promise<void> {
  logger.info(
      `New deposit event found at block ${event.block.block.header.number.toString()}`
  );

  // Get data from the event
  // The balances.transfer event has the following payload \[from, to, value\]
  // logger.info(JSON.stringify(event));
  const {
    event: {
      data: [to, amount],
    },
  } = event;

  const blockNumber: number = event.block.block.header.number.toNumber();


  const balanceChange = BalanceChange.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}`,
    block_height: blockNumber,
    timestamp: event.block.timestamp,
    event: "deposit",
    address: to.toString(),
    balance: (amount as Balance).toBigInt(),
  });

  await Promise.all([balanceChange.save()]);
}

export async function handleBalanceSet(event: SubstrateEvent): Promise<void> {
  logger.info(
      `New BalanceSet event found at block ${event.block.block.header.number.toString()}`
  );

  // Get data from the event
  // The balances.transfer event has the following payload \[from, to, value\]
  // logger.info(JSON.stringify(event));
  const {
    event: {
      data: [who, amount],
    },
  } = event;

  const blockNumber: number = event.block.block.header.number.toNumber();


  const balanceChange = BalanceChange.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}`,
    block_height: blockNumber,
    timestamp: event.block.timestamp,
    event: "balanceSet",
    address: who.toString(),
    balance: (amount as Balance).toBigInt(),
  });

  await Promise.all([balanceChange.save()]);
}

