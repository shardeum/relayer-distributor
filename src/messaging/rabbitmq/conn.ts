import { Connection } from 'amqplib'
import * as amqp from 'amqplib'

export default class RMQConnection {
  conn: Connection
  name!: string

  constructor(name: string) {
    this.name = name
  }

  public async getConnection(): Promise<Connection> {
    this.conn = await amqp.connect({
      protocol: process.env.RMQ_PROTOCOL || 'amqp',
      hostname: process.env.RMQ_HOST,
      port: process.env.RMQ_PORT ? parseInt(process.env.RMQ_PORT) : 5672,
      username: process.env.RMQ_USER,
      password: process.env.RMQ_PASS,
    })

    return this.conn
  }
}
