import { cpus } from 'os'
import { join } from 'path'
import * as Logger from './Logger'
import * as dbstore from './dbstore'
import type { Worker } from 'cluster'
import * as clusterModule from 'cluster'
import { initHttpServer } from './child-process'
import { setHashKey, initLogger } from './utils'
import { config, overrideDefaultConfig } from './Config'

import {
  workerClientMap,
  workerProcessMap,
  refreshSubscribers,
  updateConfigAndSubscriberList,
  registerWorkerMessageListener,
} from './distributor/utils'

const cluster = clusterModule as unknown as clusterModule.Cluster
// Override default config params from config file, env vars, and cli args
const file = join(process.cwd(), 'distributor-config.json')
const { argv, env } = process

// const NUMBER_OF_WORKERS = cpus().length
const NUMBER_OF_WORKERS = 2

const initDistributor = async (): Promise<void> => {
  // Common logic for both parent and worker processes
  overrideDefaultConfig(file, env, argv)

  // Set crypto hash keys from config
  const hashKey = config.DISTRIBUTOR_HASH_KEY
  setHashKey(hashKey)
  initLogger()
  addSigListeners()
  updateConfigAndSubscriberList()

  if (cluster.isPrimary) {
    // Primary/Parent Process Logic
    Logger.mainLogger.debug(`Distributor Master Process (${process.pid}) Started`)
    for (let i = 0; i < NUMBER_OF_WORKERS; i++) {
      const worker: Worker = cluster.fork()
      workerClientMap.set(worker, [])
      workerProcessMap.set(worker.process.pid, worker)
      registerWorkerMessageListener(worker)
      if (config.limitToSubscribersOnly) refreshSubscribers()
      console.log(`⛏️ Worker ${worker.process.pid} started`)
    }

    cluster.on('exit', (worker: Worker) => {
      Logger.mainLogger.debug(`Worker Process (${worker}) Terminated`)
    })
  } else {
    // Worker Process Logic
    await dbstore.initializeDB(config)
    const { worker } = cluster
    await initHttpServer(worker)
  }
}

const addSigListeners = (): void => {
  process.on('SIGUSR1', async () => {
    // Reload the distributor-config.json
    overrideDefaultConfig(file, env, argv)
    Logger.mainLogger.debug('Config reloaded', config)
    Logger.mainLogger.debug('DETECTED SIGUSR1 SIGNAL @: ', process.pid)
    console.log('DETECTED SIGUSR1 SIGNAL @: ', process.pid)
    if (cluster.isPrimary) {
      // Check for expired subscribers in the updated config
      if (config.limitToSubscribersOnly) refreshSubscribers()
    } else {
      // Refresh the list of subscribers in every worker process
      updateConfigAndSubscriberList()
    }
  })
  process.on('SIGINT', async () => {
    console.log('Exiting on SIGINT')
    await dbstore.closeDatabase()
    process.exit(0)
  })
  process.on('SIGTERM', async () => {
    console.log('Exiting on SIGTERM')
    await dbstore.closeDatabase()
    process.exit(0)
  })
  process.on('uncaughtException', (error) => {
    console.error('Uncaught Exception in Distributor: ', error)
  })
  Logger.mainLogger.debug('Registered signal listeners.')
}

initDistributor()
