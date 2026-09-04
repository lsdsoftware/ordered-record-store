import { parse as parseUuid, v7 as uuidv7 } from 'uuid'

import { invalidArgument } from './validation.js'

const UUID_BYTES = 16
const CURSOR_BYTES = UUID_BYTES * 2
const CURSOR_LENGTH = 43

export interface ParsedRecordId {
  readonly recordBytes: Buffer
  readonly segmentBytes: Buffer
  readonly recordHex: string
  readonly segmentHex: string
}

export interface GeneratedRecordIdentity extends ParsedRecordId {
  readonly id: string
  readonly createdAtMilliseconds: number
}

export function generateRecordIdentity(segmentBytes?: Uint8Array): GeneratedRecordIdentity {
  const recordBytes = Buffer.from(parseUuid(uuidv7()))
  const actualSegmentBytes = segmentBytes === undefined
    ? recordBytes
    : Buffer.from(segmentBytes)
  assertUuidV7(actualSegmentBytes, 'segment UUID')
  const id = Buffer.concat([recordBytes, actualSegmentBytes]).toString('base64url')
  const parsed = parseRecordId(id)
  return {
    ...parsed,
    id,
    createdAtMilliseconds: uuidV7Timestamp(recordBytes),
  }
}

export function parseRecordId(value: string, name = 'recordId'): ParsedRecordId {
  if (
    typeof value !== 'string'
    || value.length !== CURSOR_LENGTH
    || !/^[A-Za-z0-9_-]+$/.test(value)
  ) {
    throw invalidArgument(`${name} must be a canonical ordered-record cursor`)
  }

  const bytes = Buffer.from(value, 'base64url')
  if (bytes.length !== CURSOR_BYTES || bytes.toString('base64url') !== value) {
    throw invalidArgument(`${name} must be a canonical ordered-record cursor`)
  }

  const recordBytes = bytes.subarray(0, UUID_BYTES)
  const segmentBytes = bytes.subarray(UUID_BYTES)
  assertUuidV7(recordBytes, `${name} record UUID`)
  assertUuidV7(segmentBytes, `${name} segment UUID`)
  return {
    recordBytes,
    segmentBytes,
    recordHex: recordBytes.toString('hex'),
    segmentHex: segmentBytes.toString('hex'),
  }
}

export function compareRecordIds(left: string, right: string): -1 | 0 | 1 {
  const comparison = Buffer.compare(
    parseRecordId(left, 'left').recordBytes,
    parseRecordId(right, 'right').recordBytes,
  )
  return comparison < 0 ? -1 : comparison > 0 ? 1 : 0
}

export function makeRecordId(recordBytes: Uint8Array, segmentBytes: Uint8Array): string {
  const record = Buffer.from(recordBytes)
  const segment = Buffer.from(segmentBytes)
  assertUuidV7(record, 'record UUID')
  assertUuidV7(segment, 'segment UUID')
  return Buffer.concat([record, segment]).toString('base64url')
}

export function uuidV7Timestamp(bytes: Uint8Array): number {
  assertUuidV7(bytes, 'UUID')
  let value = 0
  for (let index = 0; index < 6; index++) {
    value = value * 256 + bytes[index]!
  }
  return value
}

export function invertUuidHex(bytes: Uint8Array): string {
  assertUuidV7(bytes, 'segment UUID')
  const inverted = Buffer.alloc(bytes.length)
  for (let index = 0; index < bytes.length; index++) {
    inverted[index] = 0xff ^ bytes[index]!
  }
  return inverted.toString('hex')
}

export function segmentBytesFromHex(value: string): Buffer {
  if (!/^[0-9a-f]{32}$/.test(value)) {
    throw invalidArgument('segment ID must be lowercase UUID hex')
  }
  const bytes = Buffer.from(value, 'hex')
  assertUuidV7(bytes, 'segment UUID')
  return bytes
}

function assertUuidV7(bytes: Uint8Array, name: string): void {
  if (
    bytes.length !== UUID_BYTES
    || (bytes[6]! >>> 4) !== 7
    || (bytes[8]! & 0xc0) !== 0x80
  ) {
    throw invalidArgument(`${name} must be a UUIDv7`)
  }
}
