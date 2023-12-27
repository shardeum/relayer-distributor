import { Signature } from '@shardus/crypto-utils'
import * as db from './sqlite3storage'
import { extractValues, extractValuesFromArray } from './sqlite3storage'
import * as Logger from '../Logger'
import { config } from '../Config'
import { DeSerializeFromJsonString } from '../utils/serialization'

export interface Receipt {
  receiptId: string
  tx: unknown //TODO: Transaction in explorer
  cycle: number
  timestamp: number
  result: object
  beforeStateAccounts: unknown[] //TODO: WrappedAccount[] in explorer
  accounts: unknown[] //TODO: WrappedAccount[] type in explorer 
  receipt: unknown //TODO: WrappedAccount type in explorer
  sign: Signature
}

export interface ReceiptFromDB {
  receiptId: string
  tx: string
  cycle: number
  timestamp: number
  result: string
  beforeStateAccounts: string | null
  accounts: string
  receipt: string | null
  sign: string
}

export async function insertReceipt(receipt: Receipt): Promise<void> {
  try {
    const fields = Object.keys(receipt).join(', ')
    const placeholders = Object.keys(receipt).fill('?').join(', ')
    const values = extractValues(receipt)
    const sql = 'INSERT OR REPLACE INTO receipts (' + fields + ') VALUES (' + placeholders + ')'
    await db.run(sql, values)
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Successfully inserted Receipt', receipt.receiptId)
    }
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error(
      'Unable to insert Receipt or it is already stored in to database',
      receipt.receiptId
    )
  }
}

export async function bulkInsertReceipts(receipts: Receipt[]): Promise<void> {
  try {
    const fields = Object.keys(receipts[0]).join(', ')
    const placeholders = Object.keys(receipts[0]).fill('?').join(', ')
    const values = extractValuesFromArray(receipts)
    let sql = 'INSERT OR REPLACE INTO receipts (' + fields + ') VALUES (' + placeholders + ')'
    for (let i = 1; i < receipts.length; i++) {
      sql = sql + ', (' + placeholders + ')'
    }
    await db.run(sql, values)
    Logger.mainLogger.debug('Successfully inserted Receipts', receipts.length)
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error('Unable to bulk insert Receipts', receipts.length)
  }
}

export async function queryReceiptByReceiptId(receiptId: string): Promise<Receipt | void> {
  try {
    const sql = `SELECT * FROM receipts WHERE receiptId=?`
    const receipt = await db.get(sql, [receiptId]) as ReceiptFromDB
    if (receipt) {
      if (receipt.tx) receipt.tx = DeSerializeFromJsonString(receipt.tx)
      if (receipt.beforeStateAccounts)
        receipt.beforeStateAccounts = DeSerializeFromJsonString(receipt.beforeStateAccounts)
      if (receipt.accounts) receipt.accounts = DeSerializeFromJsonString(receipt.accounts)
      if (receipt.receipt) receipt.receipt = DeSerializeFromJsonString(receipt.receipt)
      if (receipt.result) receipt.result = DeSerializeFromJsonString(receipt.result) 
      if (receipt.sign) receipt.sign = DeSerializeFromJsonString(receipt.sign)
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Receipt receiptId', receipt)
    }
    return receipt as unknown as Receipt
  } catch (e) {
    Logger.mainLogger.error(e)
  }
}

export async function queryLatestReceipts(count: number): Promise<Receipt[] | void> {
  try {
    const sql = `SELECT * FROM receipts ORDER BY cycle DESC, timestamp DESC LIMIT ${count ? count : 100}`
    const receipts= await db.all(sql) as ReceiptFromDB[]
    if (receipts.length > 0) {
      receipts.forEach((receipt: ReceiptFromDB) => {
        if (receipt.tx) receipt.tx = DeSerializeFromJsonString(receipt.tx)
        if (receipt.beforeStateAccounts)
          receipt.beforeStateAccounts = DeSerializeFromJsonString(receipt.beforeStateAccounts)
        if (receipt.accounts) receipt.accounts = DeSerializeFromJsonString(receipt.accounts)
        if (receipt.receipt) receipt.receipt = DeSerializeFromJsonString(receipt.receipt)
        if (receipt.result) receipt.result = DeSerializeFromJsonString(receipt.result)
        if (receipt.sign) receipt.sign = DeSerializeFromJsonString(receipt.sign)
      })
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Receipt latest', receipts)
    }
    return receipts as unknown as Receipt[]
  } catch (e) {
    Logger.mainLogger.error(e)
  }
}

export async function queryReceipts(skip = 0, limit = 10000): Promise<Receipt[] | void> {
  let receipts
  try {
    const sql = `SELECT * FROM receipts ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    receipts = await db.all(sql)
    if (receipts.length > 0) {
      receipts.forEach((receipt: ReceiptFromDB) => {
        if (receipt.tx) receipt.tx = DeSerializeFromJsonString(receipt.tx)
        if (receipt.beforeStateAccounts)
          receipt.beforeStateAccounts = DeSerializeFromJsonString(receipt.beforeStateAccounts)
        if (receipt.accounts) receipt.accounts = DeSerializeFromJsonString(receipt.accounts)
        if (receipt.receipt) receipt.receipt = DeSerializeFromJsonString(receipt.receipt)
        if (receipt.result) receipt.result = DeSerializeFromJsonString(receipt.result)
        if (receipt.sign) receipt.sign = DeSerializeFromJsonString(receipt.sign)
      })
    }
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('Receipt receipts', receipts ? receipts.length : receipts, 'skip', skip)
  }
  return receipts
}

export async function queryReceiptCount(): Promise<number> {
  let receipts
  try {
    const sql = `SELECT COUNT(*) FROM receipts`
    receipts = await db.get(sql, [])
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('Receipt count', receipts)
  }
  if (receipts) receipts = receipts['COUNT(*)']
  else receipts = 0
  return receipts
}

export async function queryReceiptCountByCycles(start: number, end: number): Promise<Array<{cycle: number, receipts: number}> | void> {
  let receipts
  try {
    const sql = `SELECT cycle, COUNT(*) FROM receipts GROUP BY cycle HAVING cycle BETWEEN ? AND ? ORDER BY cycle ASC`
    receipts = await db.all(sql, [start, end])
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('Receipt count by cycle', receipts)
  }
  if (receipts.length > 0) {
    receipts.forEach((receipt) => {
      receipt['receipts'] = receipt['COUNT(*)']
      delete receipt['COUNT(*)']
    })
  }
  return receipts
}

export async function queryReceiptCountBetweenCycles(startCycleNumber: number, endCycleNumber: number): Promise<number | void> {
  let receipts
  try {
    const sql = `SELECT COUNT(*) FROM receipts WHERE cycle BETWEEN ? AND ?`
    receipts = await db.get(sql, [startCycleNumber, endCycleNumber])
  } catch (e) {
    console.log(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('Receipt count between cycles', receipts)
  }
  if (receipts) receipts = receipts['COUNT(*)']
  else receipts = 0
  return receipts
}

export async function queryReceiptsBetweenCycles(
  skip = 0,
  limit = 10000,
  startCycleNumber: number,
  endCycleNumber: number
): Promise<Receipt[] | void> {
  let receipts
  try {
    const sql = `SELECT * FROM receipts WHERE cycle BETWEEN ? AND ? ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    receipts = await db.all(sql, [startCycleNumber, endCycleNumber])
    if (receipts.length > 0) {
      receipts.forEach((receipt: ReceiptFromDB) => {
        if (receipt.tx) receipt.tx = DeSerializeFromJsonString(receipt.tx)
        if (receipt.beforeStateAccounts)
          receipt.beforeStateAccounts = DeSerializeFromJsonString(receipt.beforeStateAccounts)
        if (receipt.accounts) receipt.accounts = DeSerializeFromJsonString(receipt.accounts)
        if (receipt.receipt) receipt.receipt = DeSerializeFromJsonString(receipt.receipt)
        if (receipt.result) receipt.result = DeSerializeFromJsonString(receipt.result)
        if (receipt.sign) receipt.sign = DeSerializeFromJsonString(receipt.sign)
      })
    }
  } catch (e) {
    console.log(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug(
      'Receipt receipts between cycles',
      receipts ? receipts.length : receipts,
      'skip',
      skip
    )
  }
  return receipts
}
