import { Config } from '../Config'
import { Database } from 'sqlite3'
import { close, readFromDB } from './sqlite3storage'

export let cycleDatabase: Database
export let accountDatabase: Database
export let transactionDatabase: Database
export let receiptDatabase: Database
export let originalTxDataDatabase: Database

export const initializeDB = async (config: Config): Promise<void> => {
  accountDatabase = await readFromDB(`${config.ARCHIVER_DB_DIR}/${config.ARCHIVER_DATA.accountDB}`, 'Account')
  cycleDatabase = await readFromDB(`${config.ARCHIVER_DB_DIR}/${config.ARCHIVER_DATA.cycleDB}`, 'Cycle')
  transactionDatabase = await readFromDB(
    `${config.ARCHIVER_DB_DIR}/${config.ARCHIVER_DATA.transactionDB}`,
    'Transaction'
  )
  receiptDatabase = await readFromDB(`${config.ARCHIVER_DB_DIR}/${config.ARCHIVER_DATA.receiptDB}`, 'Receipt')
  originalTxDataDatabase = await readFromDB(
    `${config.ARCHIVER_DB_DIR}/${config.ARCHIVER_DATA.originalTxDataDB}`,
    'OriginalTxData'
  )
  // TODO: Check if the database have the required tables and they are in the same format as the original version in the archiver
}

export const closeDatabases = async (): Promise<void> => {
  await close(cycleDatabase, 'Cycle')
  await close(accountDatabase, 'Account')
  await close(receiptDatabase, 'Receipt')
  await close(transactionDatabase, 'Transaction')
  await close(originalTxDataDatabase, 'OriginalTxData')
}
