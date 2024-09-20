import { CycleRecord } from '@shardus/types/build/src/p2p/CycleCreatorTypes'
import { distributorMode } from '../Config'
import { CheckpointDao } from '../dbstore/checkpoints'
import { queryLatestCycleRecords } from '../dbstore/cycles'
import RMQDataPublisher from './rmq_data_publisher'
import axios, { AxiosError } from 'axios'

class DistributorHealthResp {
  lastCycleInDistributorDB: number | null
  lastCyclePublishedByDistributor: number | null
  lastCycleCursorUpdatedTimestamp: number | null
}

class CollectorHealthResp {
  cycles: RMQMetric[]
  originalTxs: RMQMetric[]
  receipts: RMQMetric[]
}

class RMQMetric {
  name: string
  messages: number
  memory: number

  constructor(name: string, messages: number, memory: number) {
    this.name = name
    this.messages = messages
    this.memory = memory
  }
}

export default class RMQModeHeathCheck {
  private rmqPublisher: RMQDataPublisher

  constructor(rmqPublisher: RMQDataPublisher) {
    this.rmqPublisher = rmqPublisher
  }

  public async distributorHealth(): Promise<DistributorHealthResp> {
    const cycles = (await queryLatestCycleRecords(1)) as CycleRecord[]
    const checkpoint = await CheckpointDao.getCheckpoint('cycle', distributorMode.MQ)

    const distributorHealthResp = new DistributorHealthResp()
    if (cycles.length > 0) {
      distributorHealthResp.lastCycleInDistributorDB = cycles[0].counter
    }

    if (checkpoint != null) {
      distributorHealthResp.lastCyclePublishedByDistributor = parseInt(checkpoint.cursor)
    }

    distributorHealthResp.lastCycleCursorUpdatedTimestamp = this.rmqPublisher.getCursorUpdatedAt()

    return distributorHealthResp
  }

  public async collectorHealth(): Promise<CollectorHealthResp> {
    const collectorHealthResp = new CollectorHealthResp()
    collectorHealthResp.cycles = []
    collectorHealthResp.receipts = []
    collectorHealthResp.originalTxs = []

    /*
    - get queues bound to exchanges where distributor publishes messages
    - for all queues, get metrics and format the response
    */

    const queueBindingsForCycles = await this.getQueues(process.env.RMQ_CYCLES_EXCHANGE_NAME)
    const queueBindingsForReceipts = await this.getQueues(process.env.RMQ_RECEIPTS_EXCHANGE_NAME)
    const queueBindingsForTxs = await this.getQueues(process.env.RMQ_ORIGINAL_TXS_EXCHANGE_NAME)

    for (const queue of queueBindingsForCycles) {
      const metrics = await this.getQueueMetrics(queue)
      if (metrics !== null) {
        collectorHealthResp.cycles.push(metrics)
      }
    }
    for (const queue of queueBindingsForReceipts) {
      const metrics = await this.getQueueMetrics(queue)
      if (metrics !== null) {
        collectorHealthResp.receipts.push(metrics)
      }
    }
    for (const queue of queueBindingsForTxs) {
      const metrics = await this.getQueueMetrics(queue)
      if (metrics !== null) {
        collectorHealthResp.originalTxs.push(metrics)
      }
    }

    return collectorHealthResp
  }

  private async getQueues(exchange: string): Promise<string[]> {
    const queues = []

    const url = `${process.env.RMQ_MGMT_ENDPOINT}/api/exchanges/%2F/${exchange}/bindings/source`
    try {
      const response = await axios.get(url, {
        auth: {
          username: process.env.RMQ_USER,
          password: process.env.RMQ_PASS,
        },
      })

      const bindings = response.data
      bindings.forEach((binding: any) => {
        if (binding.destination_type === 'queue') {
          queues.push(binding.destination)
        }
      })
      return queues
    } catch (e) {
      const err = e as AxiosError
      console.log(`Error while querying queues for exchange: ${exchange}. Err: ${err.stack}`)
      return null
    }
  }

  private async getQueueMetrics(queue: string): Promise<RMQMetric> {
    try {
      const url = `${process.env.RMQ_MGMT_ENDPOINT}/api/queues/%2F/${queue}`
      const response = await axios.get<RMQMetric>(url, {
        auth: {
          username: process.env.RMQ_USER,
          password: process.env.RMQ_PASS,
        },
      })

      const { name, messages, memory } = response.data
      return new RMQMetric(name, messages, memory)
    } catch (e) {
      console.log(`Error while querying metrics for queue: ${queue}. Err: ${e}`)
      return null
    }
  }
}
