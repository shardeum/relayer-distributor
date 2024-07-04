import { ErrorObject } from 'ajv'
import { Utils } from '@shardus/types'
import { getVerifyFunction } from '../../utils/serialization/SchemaHelpers'
import { initCycleInfoReq } from './CycleInfoReq'
import { initAccountReq } from './AccountReq'
import { initReceiptReq } from './ReceiptReq'
import { initOriginalTxReq } from './OriginalTxReq'
import { initTotalDataReq } from './TotalDataReq'
import { initTransactionReq } from './TransactionReq'

export function initAjvSchemas(): void {
  // list of init req and resp of external endpoints
  initCycleInfoReq()
  initAccountReq()
  initReceiptReq()
  initOriginalTxReq()
  initTotalDataReq()
  initTransactionReq()
}

export function verifyPayload<T>(name: string, payload: T): string[] | null {
  const verifyFn = getVerifyFunction(name)
  const isValid = verifyFn(payload)
  if (!isValid) {
    return parseAjvErrors(verifyFn.errors)
  } else {
    return null
  }
}
function parseAjvErrors(errors: Array<ErrorObject> | null): string[] | null {
  if (!errors) return null

  return errors.map((error) => {
    let errorMsg = `${error.message}`
    if (error.params && Object.keys(error.params).length > 0) {
      errorMsg += `: ${Utils.safeStringify(error.params)}`
    }
    return errorMsg
  })
}
