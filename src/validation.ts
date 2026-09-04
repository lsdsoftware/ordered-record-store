import {
  MAX_DATA_BYTES,
  MAX_IDENTIFIER_BYTES,
  MAX_READ_LIMIT,
  OrderedRecordStoreError,
} from './types.js'

export function assertIdentifier(value: unknown, name: string): asserts value is string {
  if (typeof value !== 'string' || value.length === 0) {
    throw invalidArgument(`${name} must be a non-empty string`)
  }
  if (utf8Length(value) > MAX_IDENTIFIER_BYTES) {
    throw invalidArgument(`${name} must not exceed ${MAX_IDENTIFIER_BYTES} UTF-8 bytes`)
  }
}

export function assertData(value: unknown): asserts value is string {
  if (typeof value !== 'string') {
    throw invalidArgument('data must be a string')
  }
  if (utf8Length(value) > MAX_DATA_BYTES) {
    throw invalidArgument(`data must not exceed ${MAX_DATA_BYTES} UTF-8 bytes`)
  }
}

export function assertLimit(limit: number): void {
  if (!Number.isSafeInteger(limit) || limit <= 0 || limit > MAX_READ_LIMIT) {
    throw invalidArgument(`limit must be a positive safe integer no greater than ${MAX_READ_LIMIT}`)
  }
}

export function invalidArgument(message: string): OrderedRecordStoreError {
  return new OrderedRecordStoreError('INVALID_ARGUMENT', message)
}

export function utf8Length(value: string): number {
  return Buffer.byteLength(value, 'utf8')
}
