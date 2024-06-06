import { Utils as StringUtils } from '@shardus/types'

export function SerializeToJsonString(obj: unknown): string {
  try {
    return StringUtils.safeStringify(obj)
  } catch (e) {
    console.log('Error serializing object', e)
    console.log(obj)
    throw e
  }
}

export function DeSerializeFromJsonString<T>(jsonString: string): T {
  try {
    return <T>StringUtils.safeJsonParse(jsonString)
  } catch (e) {
    console.log('Error deserializing object', e)
    console.log(jsonString)
    throw e
  }
}
