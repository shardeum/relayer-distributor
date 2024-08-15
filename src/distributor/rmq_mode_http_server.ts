import * as http from 'http'
import * as Logger from '../Logger'

import { config } from '../Config'
import fastifyCors from '@fastify/cors'
import Fastify, { FastifyInstance } from 'fastify'
import fastifyRateLimit from '@fastify/rate-limit'
import { Utils as StringUtils } from '@shardus/types'
import { registerRoutes } from '../api'
import RMQModeHeathCheck from './rmq_mode_health_check'

let httpServer: http.Server

export const initRMQModeHttpServer = async (rmqHealthCheck: RMQModeHeathCheck): Promise<void> => {
  const serverFactory = (
    handler: (req: http.IncomingMessage, res: http.ServerResponse) => void
  ): http.Server => {
    httpServer = http.createServer((req, res) => {
      handler(req, res)
    })
    return httpServer
  }

  const fastifyServer = Fastify({ serverFactory })
  await fastifyServer.register(fastifyCors)
  await fastifyServer.register(fastifyRateLimit, {
    global: true,
    max: config.RATE_LIMIT,
    timeWindow: 10,
    allowList: ['127.0.0.1', '0.0.0.0'], // Excludes local IPs from rate limits
  })

  await fastifyServer.register(function (fastify, opts, done) {
    fastify.get('/distributor/is-healthy', async (_, res) => {
      const distributorHealth = await rmqHealthCheck.distributorHealth()
      return res.status(200).send(distributorHealth)
    })

    fastify.get('/collector/is-healthy', async (_, res) => {
      const collectorHealth = await rmqHealthCheck.collectorHealth()
      return res.status(200).send(collectorHealth)
    })

    done()
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
  fastifyServer.ready(() => {
    httpServer.listen(config.MQ_DISTRIBUTOR_SERVER_PORT, () => {
      Logger.mainLogger.info(`MQ Distributor-Server listening on port ${config.MQ_DISTRIBUTOR_SERVER_PORT}.`)
      return
    })

    httpServer.on('error', (err) => {
      Logger.mainLogger.error('MQ Distributor server failed to start.', err)
      process.exit(1)
    })
  })
}
