// This script is used to test the WebSocket connection to a server.
// It simulates a client sending signed data to the server and logs the server's responses.

import * as crypto from '@shardeum-foundation/lib-crypto-utils'
import * as WebSocket from 'ws'
import { Utils as StringUtils } from '@shardeum-foundation/lib-types'

// Initialize the cryptographic utility library with a specific key.
crypto.init('69fa4195670576c0160d660c3be36556ff8d504725be8a59b5a96509e0c994bc')

// Define a developer account with a public key and secret key.
const devAccount = {
  publicKey: 'YOUR_PUBLIC_KEY_HERE',
  secretKey: 'YOUR_SECRET_KEY_HERE',
}

// Define the data to be sent to the server.
const data: any = {
  count: 100,
  sender: devAccount.publicKey,
  collectorInfo: {
    publicKey: '', // Check the collector's config file for the public key
    // Check the collector's config file for the secret key
    secretKey: '',
  },
}

// Sign the data with the developer account's secret key and public key.
crypto.signObj(data, devAccount.secretKey, devAccount.publicKey)

// Convert the data to a URL-encoded JSON string.
const dataString = encodeURIComponent(StringUtils.safeStringify(data))

// Open a WebSocket connection to the server and include the data as a query parameter in the URL.
const ws = new WebSocket(`ws://127.0.0.1:6100?data=${dataString}`)

// Log when the WebSocket connection is opened.
ws.on('open', function open() {
  console.log('WebSocket connection opened')
})

// Log any data received from the server.
ws.on('message', function incoming(data) {
  console.log(`Received: ${data}`)
})

// Log any errors with the WebSocket connection.
ws.on('error', function error(err) {
  console.log(`WebSocket error: ${err}`)
})
