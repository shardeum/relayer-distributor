import * as http from 'http'
import * as Logger from '../Logger'

import { config } from '../Config'
import { Server, IncomingMessage, ServerResponse } from 'http'
import fastifyCors from '@fastify/cors'
import fastify, { FastifyInstance } from 'fastify'
import fastifyRateLimit from '@fastify/rate-limit'
import { Utils as StringUtils } from '@shardus/types'
import { registerRoutes } from '../api'
import RMQModeHeathCheck from './rmq_mode_health_check'

export const initRMQModeHttpServer = async (rmqHealthCheck: RMQModeHeathCheck): Promise<void> => {
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
  fastifyServer.listen(
    {
      port: config.MQ_DISTRIBUTOR_SERVER_PORT,
      host: '0.0.0.0',
    },
    (err) => {
      Logger.mainLogger.debug('MQ Distributor-Server listening on port', config.MQ_DISTRIBUTOR_SERVER_PORT)
      if (err) {
        fastifyServer.log.error(err)
        process.exit(1)
      }
      Logger.mainLogger.debug('MQ Distributor-Server started')
    }
  )
}
