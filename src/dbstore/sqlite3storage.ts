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
let db: Database

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

export async function init(config: Config): Promise<void> {
  console.log(config.ARCHIVER_DB_PATH)
  const dbName = `${config.ARCHIVER_DB_PATH}`
  db = new sqlite3.Database(dbName)
  await run('PRAGMA journal_mode=WAL')
  console.log('Database initialized.')
}

/**
 * Closes Database Connection Gracefully
 */
export async function close(): Promise<void> {
  try {
    console.log('Terminating Database Connections...')
    await new Promise<void>((resolve, reject) => {
      db.close((err) => {
        if (err) {
          console.log('Error closing database connection')
          console.log(err)
          reject(err)
        } else {
          console.log('Database connection closed.')
          resolve()
        }
      })
    })
  } catch (err) {
    console.error('Error thrown in db close() function: ')
    console.error(err)
  }
}

export async function runCreate(createStatement: string): Promise<void> {
  await run(createStatement)
}

export async function run(sql: string, params = [] || {}): Promise<{ id: number } | Error> {
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

export async function get(sql: string, params = []): Promise<DBRecord> {
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

export async function all(sql: string, params = []): Promise<DBRecord[]> {
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
