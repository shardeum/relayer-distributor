import * as http from 'http'

import { join } from 'path'
import * as dbstore from './dbstore'
import { config, overrideDefaultConfig } from './Config'
import { Server, IncomingMessage, ServerResponse } from 'http'
import fastifyCors from '@fastify/cors'
import fastify, { FastifyInstance } from 'fastify'
import fastifyRateLimit from '@fastify/rate-limit'
import { Utils as StringUtils } from '@shardus/types'
import { registerRoutes } from './api'
import { initLogger, setHashKey } from './utils'
import * as Logger from './Logger'
import { updateConfigAndSubscriberList } from './distributor/utils'

const file = join(process.cwd(), 'distributor-config.json')
const { argv, env } = process

const addSigListeners = (): void => {
  process.on('SIGUSR1', async () => {
    // Reload the distributor-config.json
    overrideDefaultConfig(file, env, argv)
    Logger.mainLogger.debug('Config reloaded', config)
    Logger.mainLogger.debug('DETECTED SIGUSR1 SIGNAL @: ', process.pid)
    console.log('DETECTED SIGUSR1 SIGNAL @: ', process.pid)
    updateConfigAndSubscriberList()
  })

  process.on('SIGINT', async () => {
    console.log('Exiting on SIGINT')
    await dbstore.closeDatabases()
    process.exit(0)
  })
  process.on('SIGTERM', async () => {
    console.log('Exiting on SIGTERM')
    await dbstore.closeDatabases()
    process.exit(0)
  })
  process.on('uncaughtException', (error) => {
    console.error('Uncaught Exception in Distributor: ', error)
  })
  Logger.mainLogger.debug('Registered signal listeners.')
}

export const initRMQAPIServer = async (): Promise<void> => {
  overrideDefaultConfig(file, env, argv)
  setHashKey(config.DISTRIBUTOR_HASH_KEY)
  initLogger()
  addSigListeners()
  updateConfigAndSubscriberList()
  await dbstore.initializeDB(config)

  const fastifyServer: FastifyInstance<Server, IncomingMessage, ServerResponse> = fastify({
    logger: false,
  })
  await fastifyServer.register(fastifyCors)
  await fastifyServer.register(fastifyRateLimit, {
    global: true,
    max: config.RATE_LIMIT,
    timeWindow: 10,
    allowList: ['127.0.0.1', '0.0.0.0'], // Excludes local IPs from rate limits
  })

  fastifyServer.addContentTypeParser('application/json', { parseAs: 'string' }, (req, body, done) => {
    try {
      const jsonString = typeof body === 'string' ? body : body.toString('utf8')
      done(null, StringUtils.safeJsonParse(jsonString))
    } catch (err) {
      err.statusCode = 400
      done(err, undefined)
    }
  })

  fastifyServer.setReplySerializer((payload) => {
    return StringUtils.safeStringify(payload)
  })

  // Register API routes
  registerRoutes(fastifyServer as FastifyInstance<http.Server, http.IncomingMessage, http.ServerResponse>)

  // Start server and bind to port on all interfaces
  fastifyServer.listen(
    {
      port: config.DISTRIBUTOR_PORT,
      host: '0.0.0.0',
    },
    (err) => {
      Logger.mainLogger.debug('MQ Distributor-Server listening on port', config.DISTRIBUTOR_PORT)
      if (err) {
        fastifyServer.log.error(err)
        process.exit(1)
      }
      Logger.mainLogger.debug('MQ Distributor-Server started')
    }
  )
}

initRMQAPIServer()
