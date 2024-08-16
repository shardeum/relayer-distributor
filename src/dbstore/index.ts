import { Config } from '../Config'
import {
  init,
  close,
  accountDatabase,
  transactionDatabase,
  cycleDatabase,
  receiptDatabase,
  originalTxDataDatabase,
} from './sqlite3storage'

export const initializeDB = async (config: Config): Promise<void> => {
  await init(config)
  // TODO: Check if the database have the required tables and they are in the same format as the original version in the archiver
}

export const closeDatabases = async (): Promise<void> => {
  await close(cycleDatabase, 'Cycle')
  await close(accountDatabase, 'Account')
  await close(receiptDatabase, 'Receipt')
  await close(transactionDatabase, 'Transaction')
  await close(originalTxDataDatabase, 'OriginalTxData')
}
