import {
  SubstrateExtrinsic,
  SubstrateEvent,
  SubstrateBlock,
} from "@subql/types";
import {Account, BalanceChange} from "../types";
import { Balance } from "@polkadot/types/interfaces";
import { decodeAddress } from "@polkadot/util-crypto";

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

  const fromAccount = await checkAndGetAccount(from.toString(), blockNumber);
  const toAccount = await checkAndGetAccount(to.toString(), blockNumber);

  const balanceChangeFrom = BalanceChange.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}-1`,
    blockNumber,
    date: event.block.timestamp,
    event: "transfer - from",
    accountId: fromAccount.id,
    balance: -(amount as Balance).toBigInt(),
  });

  const balanceChangeTo = BalanceChange.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}-2`,
    blockNumber,
    date: event.block.timestamp,
    event: "transfer - to",
    accountId: toAccount.id,
    balance: (amount as Balance).toBigInt(),
  });

  fromAccount.lastChangeBlock = blockNumber;
  toAccount.lastChangeBlock = blockNumber;

  await Promise.all([fromAccount.save(), toAccount.save(), balanceChangeFrom.save(), balanceChangeTo.save()]);
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

  const balanceChange = BalanceChange.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}`,
    blockNumber,
    date: event.block.timestamp,
    event: "withdrawal",
    accountId: fromAccount.id,
    balance: -(amount as Balance).toBigInt(),
  });

  fromAccount.lastChangeBlock = blockNumber;

  await Promise.all([fromAccount.save(), balanceChange.save()]);
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

  const balanceChangeFrom = BalanceChange.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}-1`,
    blockNumber,
    date: event.block.timestamp,
    event: "stakeAdded - from",
    accountId: fromAccount.id,
    balance: -(amount as Balance).toBigInt(),
  });

  const balanceChangeTo = BalanceChange.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}-2`,
    blockNumber,
    date: event.block.timestamp,
    event: "stakeAdded - to",
    accountId: toAccount.id,
    balance: (amount as Balance).toBigInt(),
  });

  fromAccount.lastChangeBlock = blockNumber;
  toAccount.lastChangeBlock = blockNumber;

  await Promise.all([fromAccount.save(), toAccount.save(), balanceChangeFrom.save(), balanceChangeTo.save()]);
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

  const balanceChangeFrom = BalanceChange.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}-1`,
    blockNumber,
    date: event.block.timestamp,
    event: "stakeRemoved - from",
    accountId: fromAccount.id,
    balance: -(amount as Balance).toBigInt(),
  });

  const balanceChangeTo = BalanceChange.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}-2`,
    blockNumber,
    date: event.block.timestamp,
    event: "stakeRemoved - to",
    accountId: toAccount.id,
    balance: (amount as Balance).toBigInt(),
  });

  fromAccount.lastChangeBlock = blockNumber;
  toAccount.lastChangeBlock = blockNumber;

  await Promise.all([fromAccount.save(), toAccount.save(), balanceChangeFrom.save(), balanceChangeTo.save()]);
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

  const balanceChange = BalanceChange.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}`,
    blockNumber,
    date: event.block.timestamp,
    event: "deposit",
    accountId: toAccount.id,
    balance: (amount as Balance).toBigInt(),
  });
  toAccount.lastChangeBlock = blockNumber;

  await Promise.all([toAccount.save(), balanceChange.save()]);
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

  const balanceChange = BalanceChange.create({
    id: `${event.block.block.header.number.toNumber()}-${event.idx}`,
    blockNumber,
    date: event.block.timestamp,
    event: "balanceSet",
    accountId: whoAccount.id,
    balance: (amount as Balance).toBigInt(),
  });

  whoAccount.lastChangeBlock = blockNumber;

  await Promise.all([whoAccount.save(), balanceChange.save()]);
}

async function checkAndGetAccount(
  id: string,
  blockNumber: number
): Promise<Account> {
  let account = await Account.get(id);
  if (!account) {
    // We couldn't find the account
    account = Account.create({
      id: id,
      publicKey: decodeAddress(id).toString(),
      firstChangeBlock: blockNumber,
    });
  }
  return account;
}

