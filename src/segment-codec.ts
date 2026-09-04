import { TextDecoder } from 'node:util'

import { crc32c } from './crc32c.js'
import {
  makeRecordId,
  segmentBytesFromHex,
  uuidV7Timestamp,
  type GeneratedRecordIdentity,
} from './record-id.js'
import {
  MAX_DATA_BYTES,
  OrderedRecordStoreError,
  type StoredRecord,
} from './types.js'

const MAGIC = Buffer.from('ORSSEG01', 'ascii')
const FIXED_HEADER_BYTES = 46
const MIN_RECORD_BODY_BYTES = 32
const utf8 = new TextDecoder('utf-8', { fatal: true })

export interface DecodedSegment {
  readonly streamId: string
  readonly segmentHex: string
  readonly segmentBytes: Buffer
  readonly createdAtMilliseconds: number
  readonly headerLength: number
  readonly validLength: number
  readonly incompleteTail: boolean
  readonly records: readonly StoredRecord[]
}

export function encodeSegmentHeader(
  streamId: string,
  segmentBytes: Uint8Array,
  createdAtMilliseconds: number,
): Buffer {
  const stream = Buffer.from(streamId, 'utf8')
  const totalLength = FIXED_HEADER_BYTES + stream.length
  const header = Buffer.alloc(totalLength)
  MAGIC.copy(header, 0)
  header.writeUInt32BE(totalLength, 8)
  header.writeUInt16BE(1, 12)
  header.writeUInt8(0, 14)
  header.writeUInt8(0, 15)
  Buffer.from(segmentBytes).copy(header, 16)
  header.writeBigUInt64BE(BigInt(createdAtMilliseconds), 32)
  header.writeUInt16BE(stream.length, 40)
  stream.copy(header, 42)
  header.writeUInt32BE(crc32c(header.subarray(8, totalLength - 4)), totalLength - 4)
  return header
}

export function encodeRecordFrame(identity: GeneratedRecordIdentity, data: string): Buffer {
  const payload = Buffer.from(data, 'utf8')
  const bodyLength = MIN_RECORD_BODY_BYTES + payload.length
  const frame = Buffer.alloc(bodyLength + 8)
  frame.writeUInt32BE(bodyLength, 0)
  identity.recordBytes.copy(frame, 4)
  frame.writeBigUInt64BE(BigInt(identity.createdAtMilliseconds), 20)
  frame.writeUInt32BE(payload.length, 28)
  payload.copy(frame, 32)
  const crcOffset = 32 + payload.length
  frame.writeUInt32BE(crc32c(frame.subarray(4, crcOffset)), crcOffset)
  frame.writeUInt32BE(bodyLength, crcOffset + 4)
  return frame
}

export function decodeSegment(
  bytes: Uint8Array,
  expected?: { readonly streamId?: string; readonly segmentHex?: string },
): DecodedSegment {
  const buffer = Buffer.from(bytes)
  if (buffer.length < FIXED_HEADER_BYTES || !buffer.subarray(0, 8).equals(MAGIC)) {
    throw corrupt('invalid or incomplete segment header')
  }

  const headerLength = buffer.readUInt32BE(8)
  const streamLength = buffer.readUInt16BE(40)
  if (
    headerLength !== FIXED_HEADER_BYTES + streamLength
    || headerLength > buffer.length
    || buffer.readUInt16BE(12) !== 1
    || buffer.readUInt8(14) !== 0
    || buffer.readUInt8(15) !== 0
  ) {
    throw corrupt('unsupported or malformed segment header')
  }
  if (
    buffer.readUInt32BE(headerLength - 4)
    !== crc32c(buffer.subarray(8, headerLength - 4))
  ) {
    throw corrupt('segment header checksum mismatch')
  }

  const segmentBytes = Buffer.from(buffer.subarray(16, 32))
  const segmentHex = segmentBytes.toString('hex')
  try {
    segmentBytesFromHex(segmentHex)
  } catch {
    throw corrupt('segment header contains an invalid UUIDv7')
  }
  const createdAtMilliseconds = numberFromUInt64(buffer.readBigUInt64BE(32), 'segment timestamp')
  if (createdAtMilliseconds !== uuidV7Timestamp(segmentBytes)) {
    throw corrupt('segment UUID and timestamp disagree')
  }

  let streamId: string
  try {
    streamId = utf8.decode(buffer.subarray(42, 42 + streamLength))
  } catch {
    throw corrupt('segment stream ID is not valid UTF-8')
  }
  if (expected?.streamId !== undefined && expected.streamId !== streamId) {
    throw corrupt('segment stream ID does not match its requested stream')
  }
  if (expected?.segmentHex !== undefined && expected.segmentHex !== segmentHex) {
    throw corrupt('segment ID does not match its path or object key')
  }

  const records: StoredRecord[] = []
  let offset = headerLength
  let incompleteTail = false
  while (offset < buffer.length) {
    if (buffer.length - offset < 4) {
      incompleteTail = true
      break
    }
    const bodyLength = buffer.readUInt32BE(offset)
    if (bodyLength < MIN_RECORD_BODY_BYTES || bodyLength > MIN_RECORD_BODY_BYTES + MAX_DATA_BYTES) {
      throw corrupt(`invalid record length at byte ${offset}`)
    }
    const frameLength = bodyLength + 8
    if (offset + frameLength > buffer.length) {
      incompleteTail = true
      break
    }
    const record = decodeFrame(buffer.subarray(offset, offset + frameLength), streamId, segmentBytes)
    if (records.length > 0 && Buffer.compare(
      Buffer.from(records.at(-1)!.id, 'base64url').subarray(0, 16),
      Buffer.from(record.id, 'base64url').subarray(0, 16),
    ) >= 0) {
      throw corrupt('record IDs are not strictly increasing')
    }
    records.push(record)
    offset += frameLength
  }
  if (records.length === 0) {
    throw corrupt('segment contains no complete records')
  }
  const firstRecordBytes = Buffer.from(records[0]!.id, 'base64url').subarray(0, 16)
  if (!firstRecordBytes.equals(segmentBytes)) {
    throw corrupt('first record UUID is not the segment UUID')
  }

  return {
    streamId,
    segmentHex,
    segmentBytes,
    createdAtMilliseconds,
    headerLength,
    validLength: offset,
    incompleteTail,
    records,
  }
}

function decodeFrame(frame: Buffer, streamId: string, segmentBytes: Uint8Array): StoredRecord {
  const bodyLength = frame.readUInt32BE(0)
  const payloadLength = frame.readUInt32BE(28)
  if (
    bodyLength !== MIN_RECORD_BODY_BYTES + payloadLength
    || frame.length !== bodyLength + 8
    || frame.readUInt32BE(frame.length - 4) !== bodyLength
  ) {
    throw corrupt('record framing is inconsistent')
  }
  const crcOffset = 32 + payloadLength
  if (frame.readUInt32BE(crcOffset) !== crc32c(frame.subarray(4, crcOffset))) {
    throw corrupt('record checksum mismatch')
  }

  const recordBytes = frame.subarray(4, 20)
  let timestamp: number
  try {
    timestamp = numberFromUInt64(frame.readBigUInt64BE(20), 'record timestamp')
    if (timestamp !== uuidV7Timestamp(recordBytes)) {
      throw corrupt('record UUID and timestamp disagree')
    }
  } catch (error) {
    if (error instanceof OrderedRecordStoreError) throw error
    throw corrupt('record contains an invalid UUIDv7')
  }

  let data: string
  try {
    data = utf8.decode(frame.subarray(32, crcOffset))
  } catch {
    throw corrupt('record payload is not valid UTF-8')
  }
  return {
    id: makeRecordId(recordBytes, segmentBytes),
    streamId,
    data,
    createdAt: new Date(timestamp).toISOString(),
  }
}

function numberFromUInt64(value: bigint, name: string): number {
  const number = Number(value)
  if (!Number.isSafeInteger(number)) {
    throw corrupt(`${name} is outside JavaScript's safe integer range`)
  }
  return number
}

function corrupt(message: string): OrderedRecordStoreError {
  return new OrderedRecordStoreError('CORRUPT_DATA', message)
}
