import { createHash, randomBytes } from 'node:crypto'
import {
  mkdir,
  open,
  readdir,
  readFile,
  rename,
  truncate,
  unlink,
} from 'node:fs/promises'
import path from 'node:path'

import {
  decodeSegment,
  encodeRecordFrame,
  encodeSegmentHeader,
  type DecodedSegment,
} from './segment-codec.js'
import {
  generateRecordIdentity,
  invertUuidHex,
  parseRecordId,
  segmentBytesFromHex,
} from './record-id.js'
import {
  OrderedRecordStoreError,
  type OrderedRecordStore,
  type SegmentObject,
  type SegmentObjectStore,
  type SegmentedOrderedRecordStoreOptions,
  type StoredRecord,
} from './types.js'
import { assertData, assertIdentifier, assertLimit, invalidArgument } from './validation.js'

const DEFAULT_ROTATION_TARGET_BYTES = 256 * 1024
const DEFAULT_CHECKPOINT_DELAY_MILLISECONDS = 5 * 60 * 1000
const DEFAULT_IDLE_EVICTION_MILLISECONDS = 14 * 24 * 60 * 60 * 1000
const DEFAULT_SWEEP_INTERVAL_MILLISECONDS = 60 * 60 * 1000
const DEFAULT_RETRY_DELAY_MILLISECONDS = 30 * 1000
const DEFAULT_MAX_CONCURRENT_OBJECT_OPERATIONS = 4
const MAX_CACHED_REMOTE_SEGMENTS = 64
const MAX_CACHED_HEADS = 1024
const MAX_CACHED_LINKS = 4096
const DISCOVERY_PAGE_SIZE = 1000

interface CachedHead extends SegmentObject {
  readonly record?: StoredRecord
}

interface SegmentLinks {
  older?: string | null
  newer?: string
}

interface LocalSegment {
  readonly streamId: string
  readonly segmentHex: string
  readonly segmentBytes: Buffer
  readonly key: string
  path: string
  buffer: Buffer
  records: StoredRecord[]
  dirty: boolean
  final: boolean
  version: number
  lastAppendMilliseconds: number
  dueAt: number | null
  retryAt: number | null
  uploading: boolean
}

interface StreamState {
  readonly streamId: string
  readonly segments: LocalSegment[]
  tail: Promise<void>
  uploadWorker: Promise<void> | null
  uploadTimer: NodeJS.Timeout | null
}

interface UploadCapture {
  readonly segment: LocalSegment
  readonly data: Buffer
  readonly version: number
}

interface CachedRemoteSegment {
  readonly bytes: Buffer
  readonly decoded: DecodedSegment
}

export async function openSegmentedOrderedRecordStore(
  options: SegmentedOrderedRecordStoreOptions,
): Promise<OrderedRecordStore> {
  const store = new SegmentedOrderedRecordStore(options)
  await store.initialize()
  return store
}

class SegmentedOrderedRecordStore implements OrderedRecordStore {
  readonly #dataDirectory: string
  readonly #segmentsDirectory: string
  readonly #objectStore: SegmentObjectStore
  readonly #rotationTargetBytes: number
  readonly #checkpointDelayMilliseconds: number
  readonly #idleEvictionMilliseconds: number
  readonly #retryDelayMilliseconds: number
  readonly #objectLimiter: PriorityLimiter
  readonly #streams = new Map<string, StreamState>()
  readonly #heads = new Map<string, CachedHead | null>()
  readonly #links = new Map<string, SegmentLinks>()
  readonly #remoteSegments = new Map<string, CachedRemoteSegment>()
  readonly #sweepIntervalMilliseconds: number
  #sweepTimer: NodeJS.Timeout | null = null
  #closed = false

  constructor(options: SegmentedOrderedRecordStoreOptions) {
    if (!path.isAbsolute(options.dataDirectory)) {
      throw invalidArgument('dataDirectory must be an absolute path')
    }
    this.#dataDirectory = path.resolve(options.dataDirectory)
    this.#segmentsDirectory = path.join(this.#dataDirectory, 'v1', 'segments')
    this.#objectStore = options.objectStore
    this.#rotationTargetBytes = positiveInteger(
      options.rotationTargetBytes ?? DEFAULT_ROTATION_TARGET_BYTES,
      'rotationTargetBytes',
    )
    this.#checkpointDelayMilliseconds = nonnegativeInteger(
      options.checkpointDelayMilliseconds ?? DEFAULT_CHECKPOINT_DELAY_MILLISECONDS,
      'checkpointDelayMilliseconds',
    )
    this.#idleEvictionMilliseconds = positiveInteger(
      options.idleEvictionMilliseconds ?? DEFAULT_IDLE_EVICTION_MILLISECONDS,
      'idleEvictionMilliseconds',
    )
    this.#sweepIntervalMilliseconds = positiveInteger(
      options.sweepIntervalMilliseconds ?? DEFAULT_SWEEP_INTERVAL_MILLISECONDS,
      'sweepIntervalMilliseconds',
    )
    this.#retryDelayMilliseconds = positiveInteger(
      options.retryDelayMilliseconds ?? DEFAULT_RETRY_DELAY_MILLISECONDS,
      'retryDelayMilliseconds',
    )
    this.#objectLimiter = new PriorityLimiter(positiveInteger(
      options.maxConcurrentObjectOperations ?? DEFAULT_MAX_CONCURRENT_OBJECT_OPERATIONS,
      'maxConcurrentObjectOperations',
    ))
  }

  async initialize(): Promise<void> {
    await mkdir(this.#segmentsDirectory, { recursive: true })
    for (const filePath of await listSegmentFiles(this.#segmentsDirectory)) {
      await this.#loadLocalFile(filePath)
    }

    const now = Date.now()
    for (const stream of this.#streams.values()) {
      stream.segments.sort(compareSegments)
      const currentSegment = stream.segments.at(-1)
      for (const segment of [...stream.segments]) {
        const current = segment === currentSegment
        segment.final = !current
        if (!current && !segment.dirty) {
          await this.#removeLocalSegment(stream, segment)
        } else if (segment.dirty) {
          segment.dueAt = now
        } else if (now - segment.lastAppendMilliseconds >= this.#idleEvictionMilliseconds) {
          await this.#removeLocalSegment(stream, segment)
        }
      }
    }

    this.#sweepTimer = setInterval(() => {
      void this.#sweepIdleSegments()
    }, this.#sweepIntervalMilliseconds)
    this.#sweepTimer.unref()

    queueMicrotask(() => {
      for (const stream of this.#streams.values()) this.#kickUploadWorker(stream)
    })
  }

  async append({ streamId, data }: { streamId: string; data: string }): Promise<StoredRecord> {
    this.#assertOpen()
    assertIdentifier(streamId, 'streamId')
    assertData(data)
    const stream = this.#getStream(streamId)
    return this.#withStream(stream, async () => {
      const previousHead = await this.#getHead(stream)
      try {
        let segment = await this.#ensureCurrentLocalSegment(stream, previousHead)
        if (segment !== null && segment.buffer.length >= this.#rotationTargetBytes) {
          await this.#retireCurrentSegment(stream, segment)
          segment = null
        }
        const record = segment === null
          ? await this.#createSegmentWithRecord(stream, data)
          : await this.#appendToSegment(stream, segment, data)
        const current = stream.segments.at(-1)!
        const previousHex = previousHead === null ? null : parseObjectKey(stream.streamId, previousHead.key)
        if (previousHex !== current.segmentHex) {
          this.#rememberLink(stream, current.segmentHex, { older: previousHex })
          if (previousHex !== null) this.#rememberLink(stream, previousHex, { newer: current.segmentHex })
        }
        remember(this.#heads, stream.streamId, { key: current.key, size: current.buffer.length, record }, MAX_CACHED_HEADS)
        this.#kickUploadWorker(stream)
        return record
      } catch (error) {
        this.#heads.delete(stream.streamId)
        // Do not retain discovery assumptions across an uncertain mutation.
        for (const key of this.#links.keys()) {
          if (key.startsWith(`${stream.streamId}\0`)) this.#links.delete(key)
        }
        throw error
      }
    })
  }

  async read({ streamId, beforeId, afterId, limit }: {
    streamId: string
    beforeId?: string
    afterId?: string
    limit: number
  }): Promise<StoredRecord[]> {
    this.#assertOpen()
    assertIdentifier(streamId, 'streamId')
    assertLimit(limit)
    if (beforeId !== undefined && afterId !== undefined) {
      throw invalidArgument('beforeId and afterId are mutually exclusive')
    }
    const boundary = beforeId ?? afterId
    if (boundary !== undefined) parseRecordId(boundary)

    const stream = this.#getStream(streamId)
    return this.#withStream(stream, async () => {
      if (afterId !== undefined) {
        return this.#readAfter(stream, afterId, limit)
      }
      return this.#readBackward(stream, beforeId, limit)
    })
  }

  async getLatest(streamIds: readonly string[]): Promise<Map<string, StoredRecord | null>> {
    this.#assertOpen()
    for (const streamId of streamIds) assertIdentifier(streamId, 'streamId')
    const entries = await Promise.all(streamIds.map(async streamId => {
      const stream = this.#getStream(streamId)
      const record = await this.#withStream(stream, () => this.#getLatestForStream(stream))
      return [streamId, record] as const
    }))
    return new Map(entries)
  }

  async close(): Promise<void> {
    if (this.#closed) return
    this.#closed = true
    if (this.#sweepTimer !== null) clearInterval(this.#sweepTimer)
    for (const stream of this.#streams.values()) {
      if (stream.uploadTimer !== null) clearTimeout(stream.uploadTimer)
      stream.uploadTimer = null
    }
    await Promise.all(Array.from(this.#streams.values(), stream => stream.tail))
    await Promise.all(Array.from(this.#streams.values(), stream => stream.uploadWorker))
    await Promise.all(Array.from(this.#streams.values(), stream => stream.tail))
    this.#remoteSegments.clear()
    this.#heads.clear()
    this.#links.clear()
  }

  async #loadLocalFile(filePath: string): Promise<void> {
    const dirty = filePath.endsWith('.dirty.ors')
    let location: ReturnType<typeof parseLocalPath>
    try {
      location = parseLocalPath(this.#segmentsDirectory, filePath)
    } catch (error) {
      if (dirty) throw error
      await quarantine(filePath)
      return
    }

    let bytes: Buffer
    let decoded: DecodedSegment
    try {
      bytes = await readFile(filePath)
      decoded = decodeSegment(bytes, { segmentHex: location.segmentHex })
      assertHashLocation(decoded.streamId, location.shard, location.hashRemainder)
      if (decoded.incompleteTail) {
        if (!dirty) throw corrupt('clean segment has an incomplete tail')
        await truncate(filePath, decoded.validLength)
        await syncFile(filePath)
        bytes = bytes.subarray(0, decoded.validLength)
        decoded = decodeSegment(bytes, { segmentHex: location.segmentHex })
      }
    } catch (error) {
      if (dirty) throw error
      await quarantine(filePath)
      return
    }

    const stream = this.#getStream(decoded.streamId)
    if (stream.segments.some(segment => segment.segmentHex === decoded.segmentHex)) {
      if (dirty) throw corrupt(`duplicate local segment ${decoded.segmentHex}`)
      await quarantine(filePath)
      return
    }
    const lastAppendMilliseconds = Date.parse(decoded.records.at(-1)!.createdAt)
    stream.segments.push({
      streamId: decoded.streamId,
      segmentHex: decoded.segmentHex,
      segmentBytes: decoded.segmentBytes,
      key: objectKey(decoded.streamId, decoded.segmentBytes),
      path: filePath,
      buffer: bytes,
      records: [...decoded.records],
      dirty,
      final: false,
      version: 0,
      lastAppendMilliseconds,
      dueAt: null,
      retryAt: null,
      uploading: false,
    })
  }

  async #ensureCurrentLocalSegment(stream: StreamState, newest: SegmentObject | null): Promise<LocalSegment | null> {
    const current = stream.segments.at(-1)
    if (current !== undefined) return current

    if (newest === null || newest.size >= this.#rotationTargetBytes) return null
    const segmentHex = parseObjectKey(stream.streamId, newest.key)
    const remote = await this.#getRemoteSegment(stream.streamId, segmentHex, newest.key)
    if (remote === null) throw corrupt(`listed segment disappeared: ${newest.key}`)
    const segment = await this.#installCleanLocalSegment(stream, remote.decoded, remote.bytes)
    this.#remoteSegments.delete(newest.key)
    return segment
  }

  async #installCleanLocalSegment(
    stream: StreamState,
    decoded: DecodedSegment,
    bytes: Buffer,
  ): Promise<LocalSegment> {
    const directory = localStreamDirectory(this.#segmentsDirectory, stream.streamId)
    await mkdir(directory, { recursive: true })
    const finalPath = path.join(directory, `${decoded.segmentHex}.clean.ors`)
    const temporaryPath = path.join(
      directory,
      `${decoded.segmentHex}.tmp-${process.pid}-${randomBytes(6).toString('hex')}`,
    )
    const handle = await open(temporaryPath, 'wx')
    try {
      await handle.writeFile(bytes)
      await handle.sync()
    } finally {
      await handle.close()
    }
    await rename(temporaryPath, finalPath)
    await syncDirectory(directory)
    const segment: LocalSegment = {
      streamId: stream.streamId,
      segmentHex: decoded.segmentHex,
      segmentBytes: decoded.segmentBytes,
      key: objectKey(stream.streamId, decoded.segmentBytes),
      path: finalPath,
      buffer: bytes,
      records: [...decoded.records],
      dirty: false,
      final: false,
      version: 0,
      lastAppendMilliseconds: Date.parse(decoded.records.at(-1)!.createdAt),
      dueAt: null,
      retryAt: null,
      uploading: false,
    }
    stream.segments.push(segment)
    stream.segments.sort(compareSegments)
    return segment
  }

  async #createSegmentWithRecord(stream: StreamState, data: string): Promise<StoredRecord> {
    const identity = generateRecordIdentity()
    const header = encodeSegmentHeader(
      stream.streamId,
      identity.segmentBytes,
      identity.createdAtMilliseconds,
    )
    const frame = encodeRecordFrame(identity, data)
    const bytes = Buffer.concat([header, frame])
    const directory = localStreamDirectory(this.#segmentsDirectory, stream.streamId)
    await mkdir(directory, { recursive: true })
    const filePath = path.join(directory, `${identity.segmentHex}.dirty.ors`)
    try {
      const handle = await open(filePath, 'wx')
      try {
        await handle.writeFile(bytes)
        await handle.sync()
      } finally {
        await handle.close()
      }
      await syncDirectory(directory)
    } catch (error) {
      await unlink(filePath).catch(() => undefined)
      throw error
    }
    const record: StoredRecord = {
      id: identity.id,
      streamId: stream.streamId,
      data,
      createdAt: new Date(identity.createdAtMilliseconds).toISOString(),
    }
    const segment: LocalSegment = {
      streamId: stream.streamId,
      segmentHex: identity.segmentHex,
      segmentBytes: identity.segmentBytes,
      key: objectKey(stream.streamId, identity.segmentBytes),
      path: filePath,
      buffer: bytes,
      records: [record],
      dirty: true,
      final: false,
      version: 1,
      lastAppendMilliseconds: Date.now(),
      dueAt: Date.now() + this.#checkpointDelayMilliseconds,
      retryAt: null,
      uploading: false,
    }
    stream.segments.push(segment)
    stream.segments.sort(compareSegments)
    return record
  }

  async #appendToSegment(
    stream: StreamState,
    segment: LocalSegment,
    data: string,
  ): Promise<StoredRecord> {
    if (!segment.dirty) await this.#markDirty(segment)
    const identity = generateRecordIdentity(segment.segmentBytes)
    const frame = encodeRecordFrame(identity, data)
    const previousLength = segment.buffer.length
    const handle = await open(segment.path, 'a')
    try {
      await handle.write(frame)
      await handle.sync()
    } catch (error) {
      await handle.close().catch(() => undefined)
      await truncate(segment.path, previousLength).catch(() => undefined)
      await syncFile(segment.path).catch(() => undefined)
      throw error
    }
    await handle.close()

    const record: StoredRecord = {
      id: identity.id,
      streamId: stream.streamId,
      data,
      createdAt: new Date(identity.createdAtMilliseconds).toISOString(),
    }
    segment.buffer = Buffer.concat([segment.buffer, frame])
    segment.records.push(record)
    segment.version++
    segment.lastAppendMilliseconds = Date.now()
    if (segment.dueAt === null && segment.retryAt === null) {
      segment.dueAt = Date.now() + this.#checkpointDelayMilliseconds
    }
    return record
  }

  async #markDirty(segment: LocalSegment): Promise<void> {
    const dirtyPath = segment.path.replace(/\.clean\.ors$/, '.dirty.ors')
    await rename(segment.path, dirtyPath)
    await syncDirectory(path.dirname(segment.path))
    segment.path = dirtyPath
    segment.dirty = true
    segment.dueAt = Date.now() + this.#checkpointDelayMilliseconds
    segment.retryAt = null
  }

  async #retireCurrentSegment(stream: StreamState, segment: LocalSegment): Promise<void> {
    segment.final = true
    if (segment.dirty) {
      segment.dueAt = Date.now()
      segment.retryAt = null
      this.#kickUploadWorker(stream)
    } else {
      await this.#removeLocalSegment(stream, segment)
    }
  }

  async #readBackward(
    stream: StreamState,
    beforeId: string | undefined,
    limit: number,
  ): Promise<StoredRecord[]> {
    let segmentHex: string | null
    let recordIndex: number
    if (beforeId === undefined) {
      segmentHex = await this.#findHeadSegmentHex(stream)
      if (segmentHex === null) return []
      recordIndex = Number.POSITIVE_INFINITY
    } else {
      segmentHex = parseRecordId(beforeId).segmentHex
      recordIndex = -1
    }

    const reverse: StoredRecord[] = []
    let first = true
    while (segmentHex !== null && reverse.length < limit) {
      const decoded = await this.#loadSegment(stream, segmentHex)
      if (first && beforeId !== undefined) {
        recordIndex = decoded.records.findIndex(record => record.id === beforeId)
        if (recordIndex < 0) throw invalidBoundary()
      } else if (recordIndex === Number.POSITIVE_INFINITY || !first) {
        recordIndex = decoded.records.length
      }
      for (let index = recordIndex - 1; index >= 0 && reverse.length < limit; index--) {
        reverse.push(decoded.records[index]!)
      }
      first = false
      if (reverse.length < limit) {
        segmentHex = await this.#findOlderSegmentHex(stream, segmentHex)
      }
    }
    return reverse.reverse()
  }

  async #readAfter(stream: StreamState, afterId: string, limit: number): Promise<StoredRecord[]> {
    let segmentHex: string | null = parseRecordId(afterId).segmentHex
    const result: StoredRecord[] = []
    let first = true
    while (segmentHex !== null && result.length < limit) {
      const decoded = await this.#loadSegment(stream, segmentHex)
      let start = 0
      if (first) {
        const index = decoded.records.findIndex(record => record.id === afterId)
        if (index < 0) throw invalidBoundary()
        start = index + 1
      }
      for (let index = start; index < decoded.records.length && result.length < limit; index++) {
        result.push(decoded.records[index]!)
      }
      first = false
      if (result.length < limit) {
        segmentHex = await this.#findNewerSegmentHex(stream, segmentHex)
      }
    }
    return result
  }

  #knownHead(stream: StreamState): CachedHead | null | undefined {
    const local = stream.segments.at(-1)
    if (local !== undefined) return { key: local.key, size: local.buffer.length, record: local.records.at(-1)! }
    return recall(this.#heads, stream.streamId)
  }

  async #getHead(stream: StreamState): Promise<CachedHead | null> {
    const known = this.#knownHead(stream)
    if (known !== undefined) return known
    const objects = await this.#listObjects({ prefix: objectPrefix(stream.streamId), limit: 1 })
    const head = objects[0] ?? null
    if (head !== null) parseObjectKey(stream.streamId, head.key)
    remember(this.#heads, stream.streamId, head, MAX_CACHED_HEADS)
    return head
  }

  async #getLatestForStream(stream: StreamState): Promise<StoredRecord | null> {
    const current = stream.segments.at(-1)
    if (current !== undefined) return current.records.at(-1) ?? null
    const newest = await this.#getHead(stream)
    if (newest === null) return null
    if (newest.record !== undefined) return newest.record
    const segmentHex = parseObjectKey(stream.streamId, newest.key)
    const remote = await this.#getRemoteSegment(stream.streamId, segmentHex, newest.key)
    if (remote === null) throw corrupt(`listed segment disappeared: ${newest.key}`)
    const record = remote.decoded.records.at(-1)!
    remember(this.#heads, stream.streamId, { ...newest, record }, MAX_CACHED_HEADS)
    return record
  }

  async #findHeadSegmentHex(stream: StreamState): Promise<string | null> {
    const head = await this.#getHead(stream)
    return head === null ? null : parseObjectKey(stream.streamId, head.key)
  }

  async #loadSegment(stream: StreamState, segmentHex: string): Promise<DecodedSegment> {
    const local = stream.segments.find(segment => segment.segmentHex === segmentHex)
    if (local !== undefined) {
      return decodeSegment(local.buffer, { streamId: stream.streamId, segmentHex })
    }
    const key = objectKey(stream.streamId, segmentBytesFromHex(segmentHex))
    const remote = await this.#getRemoteSegment(stream.streamId, segmentHex, key)
    if (remote === null) throw invalidBoundary()
    const head = this.#knownHead(stream)
    if (head?.key === key) {
      remember(this.#heads, stream.streamId, { ...head, record: remote.decoded.records.at(-1)! }, MAX_CACHED_HEADS)
    }
    return remote.decoded
  }

  #rememberLink(stream: StreamState, segmentHex: string, link: SegmentLinks): void {
    const key = `${stream.streamId}\0${segmentHex}`
    remember(this.#links, key, { ...this.#links.get(key), ...link }, MAX_CACHED_LINKS)
  }

  /** Merge the fully listed key interval with unpublished local segments. */
  #rememberPage(stream: StreamState, upper: string | undefined, page: readonly SegmentObject[]): string[] {
    const remote = page.map(object => parseObjectKey(stream.streamId, object.key))
    const complete = page.length < DISCOVERY_PAGE_SIZE
    const bottom = remote.at(-1)
    const local = stream.segments.map(segment => segment.segmentHex)
      .filter(hex => (upper === undefined || hex < upper) && (complete || hex >= bottom!))
    const descending = [...new Set([...remote, ...local])].sort().reverse()
    if (upper !== undefined) descending.unshift(upper)
    for (let i = 1; i < descending.length; i++) {
      this.#rememberLink(stream, descending[i - 1]!, { older: descending[i]! })
      this.#rememberLink(stream, descending[i]!, { newer: descending[i - 1]! })
    }
    if (complete && descending.length) {
      this.#rememberLink(stream, descending.at(-1)!, { older: null })
    }
    return descending
  }

  async #findOlderSegmentHex(stream: StreamState, segmentHex: string): Promise<string | null> {
    const known = recall(this.#links, `${stream.streamId}\0${segmentHex}`)?.older
    if (known !== undefined) return known
    const page = await this.#listObjects({
      prefix: objectPrefix(stream.streamId),
      startAfter: objectKey(stream.streamId, segmentBytesFromHex(segmentHex)),
      limit: DISCOVERY_PAGE_SIZE,
    })
    return this.#rememberPage(stream, segmentHex, page)[1] ?? null
  }

  async #findNewerSegmentHex(stream: StreamState, segmentHex: string): Promise<string | null> {
    const known = recall(this.#links, `${stream.streamId}\0${segmentHex}`)?.newer
    if (known !== undefined) return known
    const head = this.#knownHead(stream)
    if (head !== undefined && head !== null && parseObjectKey(stream.streamId, head.key) === segmentHex) return null

    // Keys sort newest first. Each page proves all adjacencies in its interval,
    // so the next forward step can reuse the same discovery rather than relist.
    let upper: string | undefined
    for (;;) {
      const page = await this.#listObjects({
        prefix: objectPrefix(stream.streamId),
        startAfter: upper === undefined ? undefined : objectKey(stream.streamId, segmentBytesFromHex(upper)),
        limit: DISCOVERY_PAGE_SIZE,
      })
      const descending = this.#rememberPage(stream, upper, page)
      if (upper === undefined) {
        const local = stream.segments.at(-1)
        remember(this.#heads, stream.streamId,
          local ? { key: local.key, size: local.buffer.length } : page[0] ?? null, MAX_CACHED_HEADS)
      }
      const index = descending.indexOf(segmentHex)
      if (index >= 0) return index === 0 ? null : descending[index - 1]!
      if (page.length < DISCOVERY_PAGE_SIZE) return null
      upper = parseObjectKey(stream.streamId, page.at(-1)!.key)
    }
  }

  #kickUploadWorker(stream: StreamState): void {
    if (this.#closed || stream.uploadWorker !== null) return
    if (stream.uploadTimer !== null) {
      clearTimeout(stream.uploadTimer)
      stream.uploadTimer = null
    }
    const worker = this.#runUploadWorker(stream)
    stream.uploadWorker = worker
    void worker.finally(() => {
      if (stream.uploadWorker === worker) stream.uploadWorker = null
    })
  }

  async #runUploadWorker(stream: StreamState): Promise<void> {
    while (!this.#closed) {
      const capture = await this.#withStream(stream, () => this.#captureDueUpload(stream))
      if (capture === null) return
      try {
        const checksumSha256 = createHash('sha256').update(capture.data).digest('base64')
        await this.#objectLimiter.run(
          () => this.#objectStore.put({
            key: capture.segment.key,
            data: capture.data,
            checksumSha256,
          }),
          true,
        )
        this.#remoteSegments.delete(capture.segment.key)
        await this.#withStream(stream, () => this.#completeUpload(stream, capture))
      } catch {
        await this.#withStream(stream, () => {
          capture.segment.uploading = false
          capture.segment.retryAt = Date.now() + this.#retryDelayMilliseconds
          capture.segment.dueAt = null
          this.#scheduleUpload(stream, capture.segment.retryAt)
        })
        return
      }
    }
  }

  #captureDueUpload(stream: StreamState): UploadCapture | null {
    const segment = stream.segments
      .filter(candidate => candidate.dirty)
      .sort(compareSegments)[0]
    if (segment === undefined || segment.uploading) return null
    const readyAt = Math.max(segment.dueAt ?? 0, segment.retryAt ?? 0)
    if (readyAt > Date.now()) {
      this.#scheduleUpload(stream, readyAt)
      return null
    }
    segment.uploading = true
    segment.dueAt = null
    segment.retryAt = null
    return { segment, data: Buffer.from(segment.buffer), version: segment.version }
  }

  async #completeUpload(stream: StreamState, capture: UploadCapture): Promise<void> {
    const segment = capture.segment
    segment.uploading = false
    if (segment.version !== capture.version || segment.buffer.length !== capture.data.length) {
      if (segment.dueAt === null) {
        segment.dueAt = Date.now() + this.#checkpointDelayMilliseconds
      }
      this.#scheduleUpload(stream, segment.dueAt)
      return
    }

    const cleanPath = segment.path.replace(/\.dirty\.ors$/, '.clean.ors')
    await rename(segment.path, cleanPath)
    await syncDirectory(path.dirname(segment.path))
    segment.path = cleanPath
    segment.dirty = false
    if (
      segment.final
      || Date.now() - segment.lastAppendMilliseconds >= this.#idleEvictionMilliseconds
    ) {
      await this.#removeLocalSegment(stream, segment)
    }
  }

  #scheduleUpload(stream: StreamState, at: number): void {
    if (this.#closed) return
    if (stream.uploadTimer !== null) clearTimeout(stream.uploadTimer)
    stream.uploadTimer = setTimeout(() => {
      stream.uploadTimer = null
      this.#kickUploadWorker(stream)
    }, Math.max(0, at - Date.now()))
    stream.uploadTimer.unref()
  }

  async #sweepIdleSegments(): Promise<void> {
    if (this.#closed) return
    await Promise.all(Array.from(this.#streams.values(), stream => this.#withStream(stream, async () => {
      const current = stream.segments.at(-1)
      if (
        current === undefined
        || Date.now() - current.lastAppendMilliseconds < this.#idleEvictionMilliseconds
      ) return
      if (current.dirty) {
        current.dueAt = Date.now()
        current.retryAt = null
        this.#kickUploadWorker(stream)
      } else {
        await this.#removeLocalSegment(stream, current)
      }
    })))
  }

  async #removeLocalSegment(stream: StreamState, segment: LocalSegment): Promise<void> {
    if (stream.segments.at(-1) === segment) {
      remember(this.#heads, stream.streamId, { key: segment.key, size: segment.buffer.length, record: segment.records.at(-1)! }, MAX_CACHED_HEADS)
    }
    await unlink(segment.path).catch(error => {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error
    })
    await syncDirectory(path.dirname(segment.path))
    const index = stream.segments.indexOf(segment)
    if (index >= 0) stream.segments.splice(index, 1)
  }

  #getStream(streamId: string): StreamState {
    let stream = this.#streams.get(streamId)
    if (stream === undefined) {
      stream = {
        streamId,
        segments: [],
        tail: Promise.resolve(),
        uploadWorker: null,
        uploadTimer: null,
      }
      this.#streams.set(streamId, stream)
    }
    return stream
  }

  #withStream<T>(stream: StreamState, operation: () => Promise<T> | T): Promise<T> {
    const result = stream.tail.then(operation, operation)
    stream.tail = result.then(() => undefined, () => undefined)
    return result
  }

  async #listObjects(request: {
    prefix: string
    startAfter?: string
    limit: number
  }): Promise<readonly SegmentObject[]> {
    return this.#objectLimiter.run(() => this.#objectStore.list(request), false)
  }

  async #getRemoteSegment(
    streamId: string,
    segmentHex: string,
    key: string,
  ): Promise<CachedRemoteSegment | null> {
    const cached = this.#remoteSegments.get(key)
    if (cached !== undefined) {
      this.#remoteSegments.delete(key)
      this.#remoteSegments.set(key, cached)
      return cached
    }

    const bytes = await this.#objectLimiter.run(() => this.#objectStore.get(key), false)
    if (bytes === null) return null
    const buffer = Buffer.from(bytes)
    const decoded = decodeSegment(buffer, { streamId, segmentHex })
    if (decoded.incompleteTail) throw corrupt(`S3 segment has an incomplete tail: ${key}`)
    const remote = { bytes: buffer, decoded }
    this.#remoteSegments.set(key, remote)
    while (this.#remoteSegments.size > MAX_CACHED_REMOTE_SEGMENTS) {
      this.#remoteSegments.delete(this.#remoteSegments.keys().next().value!)
    }
    return remote
  }

  #assertOpen(): void {
    if (this.#closed) {
      throw new OrderedRecordStoreError('CLOSED', 'ordered record store is closed')
    }
  }
}

class PriorityLimiter {
  readonly #limit: number
  #active = 0
  readonly #high: Array<() => void> = []
  readonly #normal: Array<() => void> = []

  constructor(limit: number) {
    this.#limit = limit
  }

  async run<T>(operation: () => Promise<T>, highPriority: boolean): Promise<T> {
    if (this.#active >= this.#limit) {
      await new Promise<void>(resolve => {
        (highPriority ? this.#high : this.#normal).push(resolve)
      })
    }
    this.#active++
    try {
      return await operation()
    } finally {
      this.#active--
      const next = this.#high.shift() ?? this.#normal.shift()
      next?.()
    }
  }
}

function streamHash(streamId: string): string {
  return createHash('sha256').update(streamId, 'utf8').digest('hex')
}

function objectPrefix(streamId: string): string {
  const hash = streamHash(streamId)
  return `segments/v1/${hash.slice(0, 2)}/${hash.slice(2)}/`
}

function objectKey(streamId: string, segmentBytes: Uint8Array): string {
  const segmentHex = Buffer.from(segmentBytes).toString('hex')
  return `${objectPrefix(streamId)}${invertUuidHex(segmentBytes)}-${segmentHex}.ors`
}

function parseObjectKey(streamId: string, key: string): string {
  const prefix = objectPrefix(streamId)
  if (!key.startsWith(prefix)) throw corrupt(`object key is outside stream prefix: ${key}`)
  const name = key.slice(prefix.length)
  const match = /^([0-9a-f]{32})-([0-9a-f]{32})\.ors$/.exec(name)
  if (match === null) throw corrupt(`malformed segment object key: ${key}`)
  let segmentBytes: Buffer
  try {
    segmentBytes = segmentBytesFromHex(match[2]!)
  } catch {
    throw corrupt(`segment object key contains an invalid UUIDv7: ${key}`)
  }
  if (invertUuidHex(segmentBytes) !== match[1]) {
    throw corrupt(`segment object key has an invalid inverted ID: ${key}`)
  }
  return match[2]!
}

function localStreamDirectory(root: string, streamId: string): string {
  const hash = streamHash(streamId)
  return path.join(root, hash.slice(0, 2), hash.slice(2))
}

function parseLocalPath(root: string, filePath: string): {
  shard: string
  hashRemainder: string
  segmentHex: string
} {
  const parts = path.relative(root, filePath).split(path.sep)
  if (parts.length !== 3 || !/^[0-9a-f]{2}$/.test(parts[0]!) || !/^[0-9a-f]{62}$/.test(parts[1]!)) {
    throw corrupt(`malformed local segment path: ${filePath}`)
  }
  const match = /^([0-9a-f]{32})\.(?:clean|dirty)\.ors$/.exec(parts[2]!)
  if (match === null) throw corrupt(`malformed local segment filename: ${filePath}`)
  try {
    segmentBytesFromHex(match[1]!)
  } catch {
    throw corrupt(`local segment filename contains an invalid UUIDv7: ${filePath}`)
  }
  return { shard: parts[0]!, hashRemainder: parts[1]!, segmentHex: match[1]! }
}

function assertHashLocation(streamId: string, shard: string, hashRemainder: string): void {
  const expected = streamHash(streamId)
  if (expected !== `${shard}${hashRemainder}`) {
    throw corrupt('segment stream ID does not match its local hash directory')
  }
}

async function listSegmentFiles(directory: string): Promise<string[]> {
  const result: string[] = []
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const child = path.join(directory, entry.name)
    if (entry.isDirectory()) result.push(...await listSegmentFiles(child))
    else if (entry.isFile() && /\.(?:clean|dirty)\.ors$/.test(entry.name)) result.push(child)
  }
  return result
}

async function syncFile(filePath: string): Promise<void> {
  const handle = await open(filePath, 'r+')
  try {
    await handle.sync()
  } finally {
    await handle.close()
  }
}

async function syncDirectory(directory: string): Promise<void> {
  const handle = await open(directory, 'r')
  try {
    await handle.sync()
  } finally {
    await handle.close()
  }
}

async function quarantine(filePath: string): Promise<void> {
  await rename(filePath, `${filePath}.quarantine-${Date.now()}-${randomBytes(4).toString('hex')}`)
  await syncDirectory(path.dirname(filePath))
}

function compareSegments(left: LocalSegment, right: LocalSegment): number {
  return Buffer.compare(left.segmentBytes, right.segmentBytes)
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw invalidArgument(`${name} must be a positive safe integer`)
  }
  return value
}

function nonnegativeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw invalidArgument(`${name} must be a nonnegative safe integer`)
  }
  return value
}

function invalidBoundary(): OrderedRecordStoreError {
  return invalidArgument('pagination boundary must be a record from the requested stream')
}

function corrupt(message: string): OrderedRecordStoreError {
  return new OrderedRecordStoreError('CORRUPT_DATA', message)
}

/** Small LRU helpers; null is cached knowledge, undefined is a miss. */
function recall<K, V>(cache: Map<K, V>, key: K): V | undefined {
  const value = cache.get(key)
  if (value !== undefined) {
    cache.delete(key)
    cache.set(key, value)
  }
  return value
}

function remember<K, V>(cache: Map<K, V>, key: K, value: V, capacity: number): void {
  cache.delete(key)
  cache.set(key, value)
  while (cache.size > capacity) cache.delete(cache.keys().next().value!)
}
