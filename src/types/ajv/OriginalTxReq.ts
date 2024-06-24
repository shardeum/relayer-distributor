import { addSchema } from '../../utils/serialization/SchemaHelpers'

// Define the schema for ReceiptRequest
export const schemaOriginalTxReq = {
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
    sign: { owner: 'string', sig: 'string' }, // Adjust the type as per the actual definition of 'sign'
  },
  additionalProperties: false,
  required: ['sender', 'sign'],
}

// Add schema dependencies
export function addSchemaDependencies(): void {
  // No dependencies for ReceiptRequest
}

// Register schemas
export function addSchemas(): void {
  addSchema('OriginalTxReq', schemaOriginalTxReq)
}
