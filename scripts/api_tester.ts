import * as crypto from '@shardeum-foundation/lib-crypto-utils'
import fetch from 'node-fetch'
import { Utils as StringUtils } from '@shardeum-foundation/lib-types'

//run by entering command "ts-node api-tester.ts" in terminal

crypto.init('69fa4195670576c0160d660c3be36556ff8d504725be8a59b5a96509e0c994bc')

const devAccount = {
  publicKey: 'YOUR_PUBLIC_KEY_HERE',
  secretKey: 'YOUR_SECRET_KEY_HERE',
}

const data: any = {
  count: 1,
  sender: devAccount.publicKey,
}
crypto.signObj(data, devAccount.secretKey, devAccount.publicKey)
// console.log(data)

//Update endpoints name ... totalData / cycleinfo / receipt / account / transaction
fetch('http://127.0.0.1:6100/totalData', {
  // fetch('http://127.0.0.1:6100/cycleinfo', {
  // fetch('http://127.0.0.1:6100/receipt', {
  method: 'post',
  body: StringUtils.safeStringify(data),
  headers: { 'Content-Type': 'application/json' },
  timeout: 2000,
})
  .then(async (res) => {
    if (res.ok) console.log(await res.json())
    // if (res.ok) console.dir(await res.json(), { depth: null })
    else console.log(res.status)
  })
  .catch((err) => {
    console.log(err)
  })
