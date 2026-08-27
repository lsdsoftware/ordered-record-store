export type RecordId = string
export type StreamId = string

export const MAX_IDENTIFIER_BYTES = 64
export const MAX_DATA_BYTES = 65_535
export const MAX_READ_LIMIT = 1_000

export interface StoredRecord {
  readonly id: RecordId
  readonly streamId: StreamId
  readonly data: string
  /** UTC ISO-8601 timestamp with millisecond precision. */
  readonly createdAt: string
}

export interface AppendRequest {
  readonly streamId: StreamId
  readonly idempotencyKey: string
  readonly data: string
}

export interface AppendResult {
  readonly record: StoredRecord
  readonly duplicate: boolean
}

export interface ReadRequest {
  readonly streamId: StreamId
  /** Return records older than this record ID. */
  readonly beforeId?: RecordId
  /** Return records newer than this record ID. */
  readonly afterId?: RecordId
  readonly limit: number
}

export interface OrderedRecordStore {
  /**
   * Append a record and return only after it has been accepted by the store.
   * A durable implementation must not resolve until its transaction commits.
   */
  append(request: AppendRequest): Promise<AppendResult>

  /**
   * Read one stream using keyset pagination. Results are always in ascending
   * record order, including when loading the newest or an older page.
   */
  read(request: ReadRequest): Promise<StoredRecord[]>

  /** Return the most recent record for every requested stream. */
  getLatest(streamIds: readonly StreamId[]): Promise<Map<StreamId, StoredRecord | null>>

  /** Delete every record in a stream. Deleting a missing stream succeeds. */
  deleteStream(streamId: StreamId): Promise<void>
}

export type OrderedRecordStoreErrorCode =
  | 'INVALID_ARGUMENT'
  | 'IDEMPOTENCY_CONFLICT'

export class OrderedRecordStoreError extends Error {
  readonly code: OrderedRecordStoreErrorCode

  constructor(code: OrderedRecordStoreErrorCode, message: string) {
    super(message)
    this.name = 'OrderedRecordStoreError'
    this.code = code
  }
}

/**
 * Create a process-local implementation intended for development and tests.
 * All records are lost when the process exits.
 */
export function createMemoryOrderedRecordStore(): OrderedRecordStore {
  const streams = new Map<StreamId, StoredRecord[]>()
  const idempotencyIndex = new Map<StreamId, Map<string, StoredRecord>>()
  let nextId = 1n

  return {
    async append(request) {
      assertIdentifier(request.streamId, 'streamId')
      assertIdentifier(request.idempotencyKey, 'idempotencyKey')
      if (typeof request.data !== 'string') {
        throw invalidArgument('data must be a string')
      }
      if (utf8Length(request.data) > MAX_DATA_BYTES) {
        throw invalidArgument(`data must not exceed ${MAX_DATA_BYTES} UTF-8 bytes`)
      }

      let streamIndex = idempotencyIndex.get(request.streamId)
      if (!streamIndex) {
        streamIndex = new Map()
        idempotencyIndex.set(request.streamId, streamIndex)
      }

      const existing = streamIndex.get(request.idempotencyKey)
      if (existing) {
        if (existing.data !== request.data) {
          throw new OrderedRecordStoreError(
            'IDEMPOTENCY_CONFLICT',
            'idempotencyKey was already used with different data',
          )
        }
        return { record: existing, duplicate: true }
      }

      const record: StoredRecord = Object.freeze({
        id: String(nextId++),
        streamId: request.streamId,
        data: request.data,
        createdAt: new Date().toISOString(),
      })
      let stream = streams.get(request.streamId)
      if (!stream) {
        stream = []
        streams.set(request.streamId, stream)
      }
      stream.push(record)
      streamIndex.set(request.idempotencyKey, record)

      return { record, duplicate: false }
    },

    async read(request) {
      assertIdentifier(request.streamId, 'streamId')
      assertLimit(request.limit)
      if (request.beforeId !== undefined && request.afterId !== undefined) {
        throw invalidArgument('beforeId and afterId are mutually exclusive')
      }

      const beforeId = request.beforeId === undefined
        ? undefined
        : parseRecordId(request.beforeId, 'beforeId')
      const afterId = request.afterId === undefined
        ? undefined
        : parseRecordId(request.afterId, 'afterId')
      const stream = streams.get(request.streamId) ?? []

      if (afterId !== undefined) {
        return stream
          .filter(record => BigInt(record.id) > afterId)
          .slice(0, request.limit)
      }

      const eligible = beforeId === undefined
        ? stream
        : stream.filter(record => BigInt(record.id) < beforeId)
      return eligible
        .slice(Math.max(0, eligible.length - request.limit))
    },

    async getLatest(streamIds) {
      const result = new Map<StreamId, StoredRecord | null>()
      for (const streamId of streamIds) {
        assertIdentifier(streamId, 'streamId')
        const stream = streams.get(streamId)
        result.set(streamId, stream?.at(-1) ?? null)
      }
      return result
    },

    async deleteStream(streamId) {
      assertIdentifier(streamId, 'streamId')
      streams.delete(streamId)
      idempotencyIndex.delete(streamId)
    },
  }
}

function assertIdentifier(value: unknown, name: string): asserts value is string {
  if (typeof value !== 'string' || value.length === 0) {
    throw invalidArgument(`${name} must be a non-empty string`)
  }
  if (utf8Length(value) > MAX_IDENTIFIER_BYTES) {
    throw invalidArgument(`${name} must not exceed ${MAX_IDENTIFIER_BYTES} UTF-8 bytes`)
  }
}

function assertLimit(limit: number): void {
  if (!Number.isSafeInteger(limit) || limit <= 0 || limit > MAX_READ_LIMIT) {
    throw invalidArgument(`limit must be a positive safe integer no greater than ${MAX_READ_LIMIT}`)
  }
}

function parseRecordId(value: string, name: string): bigint {
  if (!/^[1-9]\d*$/.test(value)) {
    throw invalidArgument(`${name} must be a positive decimal record ID`)
  }
  return BigInt(value)
}

function invalidArgument(message: string): OrderedRecordStoreError {
  return new OrderedRecordStoreError('INVALID_ARGUMENT', message)
}

function utf8Length(value: string): number {
  return new TextEncoder().encode(value).byteLength
}
