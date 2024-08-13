import { verbose, Database } from 'sqlite3'
import * as fs from 'fs'

const sqlite3 = verbose()
let db: Database

export class Checkpoint {
  id: number | null
  data_type: string
  mode: number
  cursor: string
}

export class CheckpointDao {
  static async init(): Promise<void> {
    if (!fs.existsSync('./db')) {
      fs.mkdirSync('./db', { recursive: true })
    }

    db = new sqlite3.Database(`./db/checkpoints.db`)
    await this.run('PRAGMA journal_mode=WAL')
    console.log('[Distributor Checkpoint] Initialized.')
    const createCheckpointTableQuery = `CREATE TABLE IF NOT EXISTS data_publish_checkpoints("id" integer, "data_type" varchar NOT NULL, "mode" string NOT NULL, "cursor" string NOT NULL, PRIMARY KEY (id))`
    await this.run(createCheckpointTableQuery)

    const createDataTypeModeIndex = `CREATE UNIQUE INDEX IF NOT EXISTS "uniq_idx_data_type_mode" ON "data_publish_checkpoints" ("data_type", "mode")`
    await this.run(createDataTypeModeIndex)
  }

  static async getCheckpoint(dataType: string, mode: string): Promise<Checkpoint> {
    const query = `SELECT * FROM data_publish_checkpoints WHERE data_type = ? AND mode = ?`
    return await this.get(query, [dataType, mode])
  }

  static async upsertCheckpoint(dataType: string, mode: string, cursor: string): Promise<void> {
    const query = `INSERT INTO data_publish_checkpoints (data_type, mode, cursor) VALUES (?, ?, ?) 
                    ON CONFLICT(data_type, mode) DO UPDATE SET cursor = ?`
    await this.run(query, [dataType, mode, cursor, cursor])
  }

  static async get(sql: string, params = []): Promise<Checkpoint> {
    return new Promise((resolve, reject) => {
      db.get(sql, params, (err, result) => {
        if (err) {
          console.log('Error running sql: ' + sql)
          console.log(err)
          reject(err)
        } else {
          resolve(result as Checkpoint)
        }
      })
    })
  }

  static async run(sql: string, params = [] || {}): Promise<{ id: number } | Error> {
    return new Promise((resolve, reject) => {
      db.run(sql, params, function (err) {
        if (err) {
          console.log('[Distributor Checkpoint] Error running sql ' + sql)
          console.log(err)
          reject(err)
        } else {
          resolve({ id: this.lastID })
        }
      })
    })
  }

  static close(): void {
    db.close()
  }
}
