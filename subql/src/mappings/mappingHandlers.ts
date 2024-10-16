import {
  SubstrateExtrinsic,
  SubstrateEvent,
  SubstrateBlock,
} from "@subql/types";
import {Account, BalanceSet, Deposit, StakeAdded, StakeRemoved, Transfer, Withdrawal} from "../types";
import { Balance } from "@polkadot/types/interfaces";
import { decodeAddress } from "@polkadot/util-crypto";

export async function handleBlock(block: SubstrateBlock): Promise<void> {
  // Do something with each block handler here
}

export async function handleCall(extrinsic: SubstrateExtrinsic): Promise<void> {
  // Do something with a call handler here
}

export async function handleEvent(event: SubstrateEvent): Promise<void> {
  logger.info(
    `New transfer event found at block ${event.block.block.header.number.toString()}`
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

  const fromAccount = await checkAndGetAccount(from.toString(), blockNumber);
  const toAccount = await checkAndGetAccount(to.toString(), blockNumber);

  // Create the new transfer entity
  const transfer = Transfer.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}`,
    blockNumber,
    date: event.block.timestamp,
    fromId: fromAccount.id,
    toId: toAccount.id,
    amount: (amount as Balance).toBigInt(),
  });

  fromAccount.lastTransferBlock = blockNumber;
  toAccount.lastTransferBlock = blockNumber;

  await Promise.all([fromAccount.save(), toAccount.save(), transfer.save()]);
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

  const fromAccount = await checkAndGetAccount(from.toString(), blockNumber);

  // Create the new transfer entity
  const withdrawal = Withdrawal.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}`,
    blockNumber,
    date: event.block.timestamp,
    fromId: fromAccount.id,
    amount: (amount as Balance).toBigInt(),
  });

  fromAccount.lastTransferBlock = blockNumber;

  await Promise.all([fromAccount.save(), withdrawal.save()]);
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

  const fromAccount = await checkAndGetAccount(from.toString(), blockNumber);
  const toAccount = await checkAndGetAccount(to.toString(), blockNumber);

  // Create the new transfer entity
  const stakeAdd = StakeAdded.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}`,
    blockNumber,
    date: event.block.timestamp,
    fromId: fromAccount.id,
    toId: toAccount.id,
    amount: (amount as Balance).toBigInt(),
  });

  fromAccount.lastTransferBlock = blockNumber;
  toAccount.lastTransferBlock = blockNumber;

  await Promise.all([fromAccount.save(), toAccount.save(), stakeAdd.save()]);
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

  const fromAccount = await checkAndGetAccount(from.toString(), blockNumber);
  const toAccount = await checkAndGetAccount(to.toString(), blockNumber);

  // Create the new transfer entity
  const stakeRemove = StakeRemoved.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}`,
    blockNumber,
    date: event.block.timestamp,
    fromId: fromAccount.id,
    toId: toAccount.id,
    amount: (amount as Balance).toBigInt(),
  });

  fromAccount.lastTransferBlock = blockNumber;
  toAccount.lastTransferBlock = blockNumber;

  await Promise.all([fromAccount.save(), toAccount.save(), stakeRemove.save()]);
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

  const toAccount = await checkAndGetAccount(to.toString(), blockNumber);

  // Create the new transfer entity
  const balanceDeposit = Deposit.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}`,
    blockNumber,
    date: event.block.timestamp,
    toId: toAccount.id,
    amount: (amount as Balance).toBigInt(),
  });

  toAccount.lastTransferBlock = blockNumber;

  await Promise.all([toAccount.save(), balanceDeposit.save()]);
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

  const whoAccount = await checkAndGetAccount(who.toString(), blockNumber);

  // Create the new transfer entity
  const balanceSet = BalanceSet.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}`,
    blockNumber,
    date: event.block.timestamp,
    whoId: whoAccount.id,
    amount: (amount as Balance).toBigInt(),
  });

  whoAccount.lastTransferBlock = blockNumber;

  await Promise.all([whoAccount.save(), balanceSet.save()]);
}

async function checkAndGetAccount(
  id: string,
  blockNumber: number
): Promise<Account> {
  let account = await Account.get(id.toLowerCase());
  if (!account) {
    // We couldn't find the account
    account = Account.create({
      id: id.toLowerCase(),
      publicKey: decodeAddress(id).toString().toLowerCase(),
      firstTransferBlock: blockNumber,
    });
  }
  return account;
}

