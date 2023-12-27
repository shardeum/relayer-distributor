import * as fs from 'fs'
import * as path from 'path'
import { Config } from '../Config'
import { SerializeToJsonString } from '../utils/serialization'
import { verbose, Database } from 'sqlite3'
import { DbCycle, Cycle } from './cycles'
import { Receipt, ReceiptFromDB } from './receipts'
import { OriginalTxData } from './originalTxsData'
import { DBTransaction, Transaction } from './transactions'



const sqlite3 = verbose()
let db: Database

interface DBAccount {
  accountId: string
  data: object
  timestamp: number
  hash: string
  cycleNumber: number
  isGlobal: boolean
}

interface DBOriginalTxData {
  txId: string
  timestamp: number
  cycle: number
  originalTxData: object
  sign: object
}

type DBRecord = DBTransaction | DbCycle | DBAccount | DBOriginalTxData | Cycle | Receipt | ReceiptFromDB | OriginalTxData | DBAccount | Transaction

export async function init(config: Config): Promise<void> {
  console.log(config.ARCHIVER_DB_PATH)
  const dbName = `${config.ARCHIVER_DB_PATH}`
  db = new sqlite3.Database(dbName)
  await run('PRAGMA journal_mode=WAL')
  console.log('Database initialized.')
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
