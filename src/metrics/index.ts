import { queryCyleCount } from '../dbstore/cycles'
import { queryOriginalTxDataCount } from '../dbstore/originalTxsData'
import { queryReceiptCount } from '../dbstore/receipts'

type DataLogReaderMetrics = {
  cycle: {
    count: number
    lastUpdationTimestamp: number
  }
  receipt: {
    count: number
    lastUpdationTimestamp: number
  }
  originalTx: {
    count: number
    lastUpdationTimestamp: number
  }
}

const dataLogReaderMetrics: DataLogReaderMetrics = {
  cycle: {
    count: 0,
    lastUpdationTimestamp: Date.now(),
  },
  receipt: {
    count: 0,
    lastUpdationTimestamp: Date.now(),
  },
  originalTx: {
    count: 0,
    lastUpdationTimestamp: Date.now(),
  },
}

export const setInitialDataLogReaderMetrics = async (): Promise<void> => {
  dataLogReaderMetrics.cycle.count = await queryCyleCount()
  dataLogReaderMetrics.cycle.lastUpdationTimestamp = Date.now()

  dataLogReaderMetrics.receipt.count = await queryReceiptCount()
  dataLogReaderMetrics.receipt.lastUpdationTimestamp = Date.now()

  dataLogReaderMetrics.originalTx.count = await queryOriginalTxDataCount()
  dataLogReaderMetrics.originalTx.lastUpdationTimestamp = Date.now()
}

export const incrementCycleCount = async (): Promise<void> => {
  dataLogReaderMetrics.cycle.count += 1
  dataLogReaderMetrics.cycle.lastUpdationTimestamp = Date.now()
}

export const incrementReceiptCount = async (): Promise<void> => {
  dataLogReaderMetrics.receipt.count += 1
  dataLogReaderMetrics.receipt.lastUpdationTimestamp = Date.now()
}

export const incrementOriginalTxCount = async (): Promise<void> => {
  dataLogReaderMetrics.originalTx.count += 1
  dataLogReaderMetrics.originalTx.lastUpdationTimestamp = Date.now()
}

export const getDataLogReaderMetrics = (): DataLogReaderMetrics => {
  return dataLogReaderMetrics
}
