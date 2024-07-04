import { addSchema } from '../../utils/serialization/SchemaHelpers'
import { DistributorAjvTypeReqRespEnum } from '../enum/DistributorAjvTypeReqRespEnum'

// Define the schema for CycleInfoRequest
export const schemaCycleInfoReq = {
  type: 'object',
  properties: {
    start: { type: ['number', 'null'] },
    end: { type: ['number', 'null'] },
    count: { type: ['number', 'null'] },
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
export function initCycleInfoReq(): void {
  addSchemaDependencies()
  addSchemas()
}

// Add schema dependencies
function addSchemaDependencies(): void {
  // No dependencies for CycleInfoRequest
}

// Register schemas
function addSchemas(): void {
  addSchema(DistributorAjvTypeReqRespEnum.CycleInfoReq, schemaCycleInfoReq)
}
