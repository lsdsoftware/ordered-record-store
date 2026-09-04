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
  readonly data: string
}

export interface ReadRequest {
  readonly streamId: StreamId
  /** Return records older than this actual record ID from the same stream. */
  readonly beforeId?: RecordId
  /** Return records newer than this actual record ID from the same stream. */
  readonly afterId?: RecordId
  readonly limit: number
}

export interface OrderedRecordStore {
  /**
   * Append a record and return only after it has been accepted by the store.
   * The durable implementation resolves after the complete local frame is
   * fsynced; remote checkpointing remains asynchronous.
   */
  append(request: AppendRequest): Promise<StoredRecord>

  /** Results are always in ascending record order. */
  read(request: ReadRequest): Promise<StoredRecord[]>

  /** Return the most recent record for every requested stream. */
  getLatest(streamIds: readonly StreamId[]): Promise<Map<StreamId, StoredRecord | null>>

  /** Reject new work and settle work already accepted by this instance. */
  close(): Promise<void>
}

export interface SegmentObject {
  readonly key: string
  readonly size: number
}

/** Domain-neutral object boundary used by the segmented adapter. */
export interface SegmentObjectStore {
  list(request: {
    readonly prefix: string
    readonly startAfter?: string
    readonly limit: number
  }): Promise<readonly SegmentObject[]>

  /** Return null when the key does not exist. */
  get(key: string): Promise<Uint8Array | null>

  /** Replace the complete object at key. checksumSha256 is base64 encoded. */
  put(request: {
    readonly key: string
    readonly data: Uint8Array
    readonly checksumSha256: string
  }): Promise<void>
}

export interface SegmentedOrderedRecordStoreOptions {
  readonly dataDirectory: string
  readonly objectStore: SegmentObjectStore
  readonly rotationTargetBytes?: number
  readonly checkpointDelayMilliseconds?: number
  readonly idleEvictionMilliseconds?: number
  readonly sweepIntervalMilliseconds?: number
  readonly retryDelayMilliseconds?: number
  readonly maxConcurrentObjectOperations?: number
}

export type OrderedRecordStoreErrorCode =
  | 'INVALID_ARGUMENT'
  | 'CLOSED'
  | 'CORRUPT_DATA'

export class OrderedRecordStoreError extends Error {
  readonly code: OrderedRecordStoreErrorCode

  constructor(code: OrderedRecordStoreErrorCode, message: string) {
    super(message)
    this.name = 'OrderedRecordStoreError'
    this.code = code
  }
}
