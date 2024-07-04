import { addSchema } from '../../utils/serialization/SchemaHelpers'
import { DistributorAjvTypeReqRespEnum } from '../enum/DistributorAjvTypeReqRespEnum'

// Define the schema for ReceiptRequest
export const schemaReceiptReq = {
  type: 'object',
  properties: {
    count: { type: ['number', 'null'] },
    start: { type: ['number', 'null'] },
    end: { type: ['number', 'null'] },
    startCycle: { type: ['number', 'null'] },
    endCycle: { type: ['number', 'null'] },
    type: { type: ['string', 'null'] },
    page: { type: ['number', 'null'] },
    txId: { type: ['string', 'null'] },
    txIdList: { type: ['string', 'null'] },
    sender: { type: 'string' },
    sign: {
      type: 'object',
      properties: {
        owner: { type: 'string' },
        sig: { type: 'string' },
      },
      required: ['owner', 'sig'],
    }, // Adjust the type as per the actual definition of 'sign'
  },
  additionalProperties: true,
  required: ['sender', 'sign'],
}
export function initReceiptReq(): void {
  addSchemaDependencies()
  addSchemas()
}

// Add schema dependencies
function addSchemaDependencies(): void {
  // No dependencies for ReceiptRequest
}

// Register schemas
function addSchemas(): void {
  addSchema(DistributorAjvTypeReqRespEnum.ReceiptReq, schemaReceiptReq)
}
