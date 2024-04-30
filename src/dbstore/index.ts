import { Config } from '../Config'
import * as db from './sqlite3storage'

export const initializeDB = async (config: Config): Promise<void> => {
  await db.init(config)
  // TODO: Check if the database have the required tables and they are in the same format as the original version in the archiver
}

export const closeDatabase = async (): Promise<void> => {
  await db.close()
}