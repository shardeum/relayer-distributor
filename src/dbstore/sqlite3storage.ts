import * as fs from 'fs'
import * as path from 'path'
import { Config } from '../Config'
import { SerializeToJsonString } from '../utils/serialization'
import { verbose, Database } from 'sqlite3'
import { DBCycle, Cycle } from './cycles'
import { Receipt, DBReceipt } from './receipts'
import { OriginalTxData } from './originalTxsData'
import { DBTransaction, Transaction } from './transactions'
import { DBAccount, AccountCopy } from './accounts'

const sqlite3 = verbose()

export interface DBOriginalTxData {
  txId: string
  timestamp: number
  cycle: number
  originalTxData: object
  sign: object
}

type DBRecord =
  | DBTransaction
  | DBCycle
  | DBOriginalTxData
  | Cycle
  | Receipt
  | DBReceipt
  | OriginalTxData
  | Transaction
  | DBAccount
  | AccountCopy

export let cycleDatabase: Database
export let accountDatabase: Database
export let transactionDatabase: Database
export let receiptDatabase: Database
export let originalTxDataDatabase: Database

export async function init(config: Config): Promise<void> {
  accountDatabase = await createDB(`${config.ARCHIVER_DB_PATH}/${config.ARCHIVER_DATA.accountDB}`, 'Account')
  cycleDatabase = await createDB(`${config.ARCHIVER_DB_PATH}/${config.ARCHIVER_DATA.cycleDB}`, 'Cycle')
  transactionDatabase = await createDB(
    `${config.ARCHIVER_DB_PATH}/${config.ARCHIVER_DATA.transactionDB}`,
    'Transaction'
  )
  receiptDatabase = await createDB(`${config.ARCHIVER_DB_PATH}/${config.ARCHIVER_DATA.receiptDB}`, 'Receipt')
  originalTxDataDatabase = await createDB(
    `${config.ARCHIVER_DB_PATH}/${config.ARCHIVER_DATA.originalTxDataDB}`,
    'OriginalTxData'
  )
}

const createDB = async (dbPath: string, dbName: string): Promise<Database> => {
  console.log('CreateDB: dbName: ', dbName, 'dbPath: ', dbPath)
  const db = new Database(dbPath, (err) => {
    if (err) {
      console.log('❌ Error opening database:', err)
      throw err
    }
  })
  await run(db, 'PRAGMA journal_mode=WAL')
  console.log(`✅ Database: ${dbName} initialized.`)
  return db
}
/**
 * Closes Database Connection Gracefully
 */
export async function close(db: Database, dbName: string): Promise<void> {
  try {
    console.log(`Terminating ${dbName} Database/Indexer Connections...`)
    await new Promise<void>((resolve, reject) => {
      db.close((err) => {
        if (err) {
          console.error(`Error closing ${dbName} Database Connection.`)
          console.log(err)
          reject(err)
        } else {
          console.log(`${dbName} Database connection closed.`)
          resolve()
        }
      })
    })
  } catch (err) {
    console.error(`Error thrown in ${dbName} db close() function: `)
    console.error(err)
  }
}

export async function run(db: Database, sql: string, params = [] || {}): Promise<{ id: number } | Error> {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
      if (err) {
        console.log('Error running sql ' + sql)
        console.log(err)
        reject(err)
      } else {
        resolve({ id: this.lastID })
      }
    })
  })
}

export async function get(db: Database, sql: string, params = []): Promise<DBRecord> {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, result) => {
      if (err) {
        console.log('Error running sql: ' + sql)
        console.log(err)
        reject(err)
      } else {
        resolve(result as DBRecord)
      }
    })
  })
}

export async function all(db: Database, sql: string, params = []): Promise<DBRecord[]> {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) {
        console.log('Error running sql: ' + sql)
        console.log(err)
        reject(err)
      } else {
        resolve(rows as DBRecord[])
      }
    })
  })
}

export function extractValues(object: unknown): (string | number | boolean | null)[] | void {
  try {
    const inputs: (string | number | boolean | null)[] = []
    for (const column of Object.keys(object)) {
      if (Object.prototype.hasOwnProperty.call(object, column)) {
        // eslint-disable-next-line security/detect-object-injection
        let value = object[column]
        if (typeof value === 'object') value = SerializeToJsonString(value)
        inputs.push(value as string | number | boolean | null)
      }
    }
    return inputs
  } catch (e) {
    console.log(e)
  }
}

export function extractValuesFromArray(arr: DBRecord[]): (string | number | boolean | null)[] | void {
  try {
    const inputs = []
    for (const object of arr) {
      for (const column of Object.keys(object)) {
        if (Object.prototype.hasOwnProperty.call(object, column)) {
          let value = Reflect.get(object, column)
          if (typeof value === 'object') value = SerializeToJsonString(value)
          inputs.push(value)
        }
      }
    }
    return inputs
  } catch (e) {
    console.log(e)
  }
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
function createDirectories(pathname: string): void {
  const __dirname = path.resolve()
  pathname = pathname.replace(/^\.*\/|\/?[^/]+\.[a-z]+|\/$/g, '') // Remove leading directory markers, and remove ending /file-name.extension
  // eslint-disable-next-line security/detect-non-literal-fs-filename
  fs.mkdirSync(path.resolve(__dirname, pathname), { recursive: true })
}
