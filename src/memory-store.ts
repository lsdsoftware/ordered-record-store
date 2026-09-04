import { generateRecordIdentity, parseRecordId } from './record-id.js'
import {
  OrderedRecordStoreError,
  type OrderedRecordStore,
  type StoredRecord,
} from './types.js'
import { assertData, assertIdentifier, assertLimit, invalidArgument } from './validation.js'

export function createMemoryOrderedRecordStore(): OrderedRecordStore {
  const streams = new Map<string, StoredRecord[]>()
  const segmentIds = new Map<string, Buffer>()
  let closed = false

  return {
    async append({ streamId, data }) {
      assertOpen(closed)
      assertIdentifier(streamId, 'streamId')
      assertData(data)
      const identity = generateRecordIdentity(segmentIds.get(streamId))
      segmentIds.set(streamId, identity.segmentBytes)
      const record: StoredRecord = {
        id: identity.id,
        streamId,
        data,
        createdAt: new Date(identity.createdAtMilliseconds).toISOString(),
      }
      const records = streams.get(streamId) ?? []
      records.push(record)
      streams.set(streamId, records)
      return record
    },

    async read({ streamId, beforeId, afterId, limit }) {
      assertOpen(closed)
      assertIdentifier(streamId, 'streamId')
      assertLimit(limit)
      if (beforeId !== undefined && afterId !== undefined) {
        throw invalidArgument('beforeId and afterId are mutually exclusive')
      }
      const records = streams.get(streamId) ?? []
      if (beforeId === undefined && afterId === undefined) {
        return records.slice(Math.max(0, records.length - limit))
      }
      const boundary = beforeId ?? afterId!
      parseRecordId(boundary)
      const index = records.findIndex(record => record.id === boundary)
      if (index < 0) {
        throw invalidArgument('pagination boundary must be a record from the requested stream')
      }
      return beforeId !== undefined
        ? records.slice(Math.max(0, index - limit), index)
        : records.slice(index + 1, index + 1 + limit)
    },

    async getLatest(streamIds) {
      assertOpen(closed)
      const result = new Map<string, StoredRecord | null>()
      for (const streamId of streamIds) {
        assertIdentifier(streamId, 'streamId')
        result.set(streamId, streams.get(streamId)?.at(-1) ?? null)
      }
      return result
    },

    async close() {
      closed = true
    },
  }
}

function assertOpen(closed: boolean): void {
  if (closed) {
    throw new OrderedRecordStoreError('CLOSED', 'ordered record store is closed')
  }
}
