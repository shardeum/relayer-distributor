import { Connection, ConfirmChannel } from 'amqplib'
import RMQConnection from '../messaging/rabbitmq/conn'
import { sleep } from '../utils/Utils'
import { queryCycleRecordsBetween } from '../dbstore/cycles'
import { queryReceiptsBetweenCycles, Receipt } from '../dbstore/receipts'
import { CycleData } from '@shardus/types/build/src/p2p/CycleCreatorTypes'
import { CheckpointDao } from '../dbstore/checkpoints'
import { config, distributorMode } from '../Config'
import { Utils as StringUtils } from '@shardus/types'
import * as Crypto from '../utils/Crypto'
import { OriginalTxData, queryOriginalTxsData } from '../dbstore/originalTxsData'

class TxData {
  txId: string
  txTs: number

  constructor(txId: string, txTs: number) {
    this.txId = txId
    this.txTs = txTs
  }
}

class ReceiptData {
  receiptId: string
  receiptTs: number

  constructor(receiptId: string, receiptTs: number) {
    this.receiptId = receiptId
    this.receiptTs = receiptTs
  }
}

export default class RMQDataPublisher {
  private conn: Connection | null
  private channel: ConfirmChannel | null
  private isConnected = false
  private isConnClosing = false

  private cycleCursor = 0
  private batchSize = 5
  private cursorUpdatedAt = Date.now()
  private cycleConfirmThreshold = this.batchSize - 1
  private cursorUpdateThresholdInMillis = 60_000 // 1 minute
  private checkpointDataType = 'cycle'
  private cycleVsTxMap = new Map<number, Map<string, TxData | null>>()
  private cycleVsRcptMap = new Map<number, Map<string, ReceiptData | null>>()
  private ttlInMillis = 24 * 60 * 60 * 1000 // 1 day

  async start(): Promise<void> {
    await this.connect()

    this.startInMemoryMapCleanupJob()

    // get cursor from db
    const checkpoint = await CheckpointDao.getCheckpoint(this.checkpointDataType, distributorMode.MQ)
    if (checkpoint != null && checkpoint.id != null && checkpoint.id > 0) {
      this.cycleCursor = parseInt(checkpoint.cursor)
    }

    const process = true
    while (process) {
      console.log(
        `[RMQDataPublisher] Started job to publish events to RMQ. isConnClosing: ${this.isConnClosing} | isConnected: ${this.isConnected}`
      )
      if (this.isConnClosing) {
        return
      }
      if (!this.isConnected) {
        await sleep(500)
        continue
      }

      try {
        await this.findAndPublishEvents()
        console.log(`[RMQDataPublisher] Competed job to publish events to RMQ`)
      } catch (e) {
        console.log(`❌ [RMQDataPublisher#start]: Error in publishing distributor events ${e}`)
      }
      await sleep(200)
    }
  }

  private async findAndPublishEvents(): Promise<void> {
    const start = this.cycleCursor
    const end = this.cycleCursor + this.batchSize - 1
    const cycles = (await queryCycleRecordsBetween(start, end)) || []

    if (cycles.length == 0) {
      console.log(`No cycles found for publishing: start: ${start} | end: ${end}`)
      return
    }

    console.log(`Got cycles: ${cycles.length}`)

    for (const cycle of cycles) {
      const txMap = this.cycleVsTxMap.get(cycle.counter)
      if (txMap === null || txMap === undefined) {
        this.cycleVsTxMap.set(cycle.counter, new Map<string, TxData>())
      }

      const rcptMap = this.cycleVsRcptMap.get(cycle.counter)
      if (rcptMap === null || rcptMap === undefined) {
        this.cycleVsRcptMap.set(cycle.counter, new Map<string, ReceiptData>())
      }
    }

    const transactions = []
    const receipts = []
    const fetch = true
    const limit = 1000

    while (fetch) {
      let skip = 0
      const txns = (await queryOriginalTxsData(skip, limit, start, end)) as OriginalTxData[]
      console.log(`Got txns: ${txns.length} between cycles ${start} and ${end}`)
      // check for new or updated transactions
      for (const txn of txns) {
        const txMap = this.cycleVsTxMap.get(txn.cycle)
        const txData = txMap.get(txn.txId)
        if (txData === null || txData === undefined) {
          txMap.set(txn.txId, new TxData(txn.txId, txn.timestamp))
          transactions.push(txn)
        } else if (txData.txTs < txn.timestamp) {
          transactions.push(txn)
        }
      }

      if (txns.length < limit) {
        break
      }
      skip += txns.length
    }

    while (fetch) {
      let skip = 0
      const rcpts = await queryReceiptsBetweenCycles(skip, limit, start, end)
      console.log(`Got receipts: ${rcpts.length} between cycles ${start} and ${end}`)
      // check for new or updated receipts
      for (const receipt of rcpts) {
        const receiptMap = this.cycleVsRcptMap.get(receipt.cycle)
        const receiptData = receiptMap.get(receipt.receiptId)
        if (receiptData === null || receiptData === undefined) {
          receiptMap.set(receipt.receiptId, new ReceiptData(receipt.receiptId, receipt.timestamp))
          receipts.push(receipt)
        } else if (receiptData.receiptTs < receipt.timestamp) {
          receipts.push(receipt)
        }
      }

      if (rcpts.length < limit) {
        break
      }
      skip += rcpts.length
    }

    console.log(`Publishing cycles: ${cycles.length}`)
    console.log(`Publishing transactions: ${transactions.length}`)
    console.log(`Publishing receipts: ${receipts.length}`)

    const promises = []
    promises.push(this.publishCycles(cycles as CycleData[]))
    promises.push(this.publishTransactions(transactions))
    promises.push(this.publishReceipts(receipts))
    await Promise.all(promises)

    // we wait for 3 cycles, before we move to cursor to next cycle
    // note that this.batchSize for fetching cycles should always be greater than 3 or we will never fetch new cycles
    let updateCursor = false
    if (cycles.length >= this.cycleConfirmThreshold) {
      updateCursor = true
      this.cycleCursor = cycles[cycles.length - this.cycleConfirmThreshold].counter
    }

    // update cursor
    // we won't update cursor everytime
    const diff = Date.now() - this.cursorUpdatedAt
    if (updateCursor && diff >= this.cursorUpdateThresholdInMillis) {
      CheckpointDao.upsertCheckpoint(
        this.checkpointDataType,
        distributorMode.MQ,
        JSON.stringify(this.cycleCursor)
      )
    }
  }

  private async publishCycles(cycles: CycleData[]): Promise<void> {
    const messages = []
    for (let i = 0; i < cycles.length; i++) {
      const cycle = cycles.at(i)
      messages.push({
        cycle: {
          counter: cycle.counter,
          cycleMarker: cycle.marker,
          cycleRecord: cycle,
        },
      })
    }
    await this.publishMessages(process.env.RMQ_CYCLES_EXCHANGE_NAME, messages)
  }

  private async publishTransactions(transactions: OriginalTxData[]): Promise<void> {
    const messages = []
    for (let i = 0; i < transactions.length; i++) {
      messages.push({
        originalTx: transactions.at(i),
      })
    }
    await this.publishMessages(process.env.RMQ_TRANSACTIONS_EXCHANGE_NAME, messages)
  }

  private async publishReceipts(receipts: Receipt[]): Promise<void> {
    const messages = []
    for (let i = 0; i < receipts.length; i++) {
      messages.push({
        receipt: receipts.at(i),
      })
    }
    await this.publishMessages(process.env.RMQ_RECEIPTS_EXCHANGE_NAME, messages)
  }

  private async publishMessages(exchange: string, messages: unknown[]): Promise<void> {
    let retry = 3
    let lastErr = null

    while (retry > 0) {
      retry--
      try {
        for (let i = 0; i < messages.length; i++) {
          const message = {
            signedData: Crypto.sign(
              messages.at(i),
              config.DISTRIBUTOR_SECRET_KEY,
              config.DISTRIBUTOR_PUBLIC_KEY
            ),
          }
          if (!this.isConnected) {
            return
          }
          this.channel.publish(exchange, '', Buffer.from(StringUtils.safeStringify(message.signedData)), {
            persistent: true,
            expiration: this.ttlInMillis,
          })
          // console.log(`published message on ${exchange}: ${msgStr}`)
        }
        const start = Date.now()
        await this.channel.waitForConfirms()
        const end = Date.now()
        console.log(
          `Done waiting for confirmation for messages on ${exchange} | Time taken: ${end - start} ms`
        )
        lastErr = null
        return
      } catch (e) {
        lastErr = e
        console.log(
          `❌ [${exchange.toUpperCase()} publishMessages] Error while publishing message to queue: ${e}`
        )
      }
    }
    if (lastErr != null) {
      throw lastErr
    }
  }

  private async connect(): Promise<void> {
    try {
      this.conn = await new RMQConnection('data_publisher').getConnection()
      this.conn.on('close', this.handleConnectionClose)
      this.conn.on('error', this.handleConnectionError)
      this.channel = await this.conn.createConfirmChannel()
      this.channel.on('error', this.handleConnectionError)
      this.channel.on('close', this.handleConnectionClose)
      this.isConnected = true
      console.log(`✅ [RMQDataPublisher#connect]: Successfully connected to RabbitMQ`)
    } catch (e) {
      console.log(`❌ [RMQDataPublisher#connect]: error while connecting to RabbitMQ: ${e}`)
      throw e
    }
  }

  private handleConnectionError(error: unknown): void {
    console.error(`[RMQDataPublisher#handleConnectionError]: Connection error: ${error}`)
    this.isConnected = false
  }

  private handleConnectionClose = async (): Promise<void> => {
    console.error(`[RMQDataPublisher#handleConnectionClose]: Connection closed}`)
    if (this.isConnClosing === true) {
      // this is triggered internally, possibly on SIGTERM/SIGINT; so no need to retry
      return Promise.resolve()
    }

    this.isConnected = false
    return new Promise<void>((resolve) => {
      this.retryConnection()
      resolve()
    })
  }

  private retryConnection(): void {
    let attempt = 0
    if (!this.isConnected) {
      const interval = setInterval(async () => {
        attempt++
        console.log(`[retryConnection]: (Attempt ${attempt}) intitiated connection retry...`)
        try {
          await this.connect()
          console.log(`[retryConnection]: (Attempt ${attempt}) successfully connected...`)
          this.isConnected = true
          clearInterval(interval)
        } catch (e) {
          console.log(`[retryConnection]: (Attempt ${attempt}) unsuccessul. Err: ${e}`)
        }
      }, 5000) // Wait 5 seconds before retrying
    }
  }

  async cleanUp(): Promise<void> {
    this.isConnClosing = true
    // sleep for some time to allow on going execution
    sleep(2000)
    if (this.channel != null) {
      await this.channel.close()
    }
    if (this.conn != null) {
      await this.conn.close()
    }
  }

  private async startInMemoryMapCleanupJob(): Promise<void> {
    setInterval(
      () => {
        console.log(`Started in-memory clean up job`)
        for (const key of this.cycleVsTxMap.keys()) {
          if (key < this.cycleCursor - this.cycleConfirmThreshold) {
            console.log(`Deleted txns for key from memory: ${key}`)
            this.cycleVsTxMap.delete(key)
          }
        }
        for (const key of this.cycleVsRcptMap.keys()) {
          if (key < this.cycleCursor - this.cycleConfirmThreshold) {
            console.log(`Deleted receipts for key from memory: ${key}`)
            this.cycleVsRcptMap.delete(key)
          }
        }
        console.log(`Completed in-memory clean up job`)
      },
      60 * 1000 // 1 minute
    )
  }
}
