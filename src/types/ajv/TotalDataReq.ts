import { addSchema } from '../../utils/serialization/SchemaHelpers'
import { DistributorAjvTypeReqRespEnum } from '../enum/DistributorAjvTypeReqRespEnum'

// Define the schema for ReceiptRequest
export const schemaTotalDataReq = {
  type: 'object',
  properties: {
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
export function initTotalDataReq(): void {
  addSchemaDependencies()
  addSchemas()
}

// Add schema dependencies
function addSchemaDependencies(): void {
  // No dependencies for ReceiptRequest
}

// Register schemas
function addSchemas(): void {
  addSchema(DistributorAjvTypeReqRespEnum.TotalDataReq, schemaTotalDataReq)
}
