import { initAjvSchemas, verifyPayload } from '../../../../src/types/ajv/Helpers'

describe('TotalData req test', () => {
  beforeAll(() => {
    initAjvSchemas()
  })

  describe('Data validation Cases', () => {
    const invalidObjects = [
      {
        // sender: '0x0',
        sign: {},
      },
      {
        sender: '0x0',
        // sign: {},
      },
    ]
    const otherInvalidObject = [
      {
        sender: '0x01',
        sign: '0x1',
        start: 'a',
        end: 'b',
        count: 'c',
      },
    ]
    const validObjects = [
      {
        sender: '0x0',
        sign: {
          owner: '0x1',
          sig: '0x1',
        },
      },
      {
        sender: '0x0',
        sign: {
          owner: '0x1',
          sig: '0x1',
        },
        start: 2,
        end: 0,
        count: 10,
      },
    ]
    test.each(invalidObjects)('should throw AJV error', (data) => {
      const res = verifyPayload('TotalDataReq', {
        start: 0,
        end: 0,
        count: 0,
        ...data,
      })
      console.log('res', res)
      expect(res.length).toBeGreaterThan(0)
      expect(res[0].slice(0, 40)).toContain(`should have required property`)
      // expect(res[0]).toEqual(`should have required property 'sender': {"missingProperty":"sender"}`)
    })
    test.each(otherInvalidObject)('should throw AJV error', (data) => {
      const res = verifyPayload('TotalDataReq', {
        ...data,
      })
      console.log('res', res)
      expect(res.length).toBeGreaterThan(0)
      expect(res[0].slice(0, 40)).toContain(`should be object:`)
      // expect(res[0]).toEqual(`should be number,null: {"type":"number,null"}`)
    })
    test.each(validObjects)('should have no AJV error', (data) => {
      const res = verifyPayload('TotalDataReq', {
        ...data,
      })
      console.log('res', res)
      expect(res).toEqual(null)
    })
  })
})
